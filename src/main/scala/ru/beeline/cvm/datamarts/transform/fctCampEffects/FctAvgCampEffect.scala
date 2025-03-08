package ru.beeline.cvm.datamarts.transform.fctCampEffects

import org.apache.spark.sql.functions.{avg, broadcast, col, count, first, lit, trim, when}
import org.apache.spark.sql.types.IntegerType

class FctAvgCampEffect extends FctDetailCampEffect {

  import spark.implicits._

  val dfChannels = broadcast(spark.table(TableDimDic))
    .filter(col("parent_isn") === 8 && col("parent_isn") =!= col("isn"))
    .select(
      col("isn").alias("INFO_CHANNEL_ID"),
      col("name_eng")
    );

  val df_channels_lower = dfChannels.columns.foldLeft(dfChannels)((df, colName) =>
    df.withColumnRenamed(colName, colName.toLowerCase)
  );

  val df_with_channels = df_result_with_sale_6mnths.select(
    "camp_id",
    "parent_camp_code",
    "wave_id",
    "camp_use_case_id",
    "monthly/sale 6mnths",
    "info_channel_id"
  );

  val df_with_channels_joined = df_with_channels
    .join(df_channels_lower, Seq("info_channel_id"), "left");

  val df_FCT_DETAIL_CAMP_EFFECT_w_channels_converted = df_with_channels_joined
    .withColumn("monthly/sale 6mnths", col("monthly/sale 6mnths").cast("double"));

  val df_FCT_AVG_CAMP_EFFECT = df_FCT_DETAIL_CAMP_EFFECT_w_channels_converted
    .groupBy("camp_use_case_id", "name_eng")
    .agg(
      avg("monthly/sale 6mnths").alias("monthly/sale 6mnths")
    )
    .filter(col("camp_use_case_id").isNotNull);

  val pivot = df_FCT_AVG_CAMP_EFFECT
    .groupBy("camp_use_case_id")
    .pivot("name_eng")
    .agg(first("monthly/sale 6mnths"));

  val renamedColumns = pivot.columns.map {
    case "Outbound CRM" => "Outbound"
    case "SMS Online" => "SMS"
    case "camp_use_case_id" => "ISN_USE_CASE"
    case other => other
  };

  val renamedPivot = pivot.toDF(renamedColumns: _*);

  val finalPivot = if (!renamedPivot.columns.contains("Push")) {
    renamedPivot.withColumn("Push", col("SMS"))
  } else {
    renamedPivot
  };

  val use_cases_dict = dimDict
    .filter(col("parent_isn") === 42641 && col("parent_isn") =!= col("isn"))
    .select(
      col("isn").alias("ISN_USE_CASE"),
      col("name_eng")
    )
    .withColumn("name_eng", trim(col("name_eng")));

  val products = tCampsLastPart
    .groupBy("PRODUCT_B_NEW", "USE_CASE_EXT_NEW", "USE_CASE_NEW")
    .agg(
      count("camp_id").alias("camp_id_count")
    )
    .withColumn("USE_CASE_NEW", trim(col("USE_CASE_NEW")))
    .withColumn("PRODUCT_B_NEW", trim(col("PRODUCT_B_NEW")))
    .withColumn("USE_CASE_EXT_NEW", trim(col("USE_CASE_EXT_NEW")));

  val dfSale = finalPivot
    .withColumn("ISN_USE_CASE", col("ISN_USE_CASE").cast(IntegerType));

  val useCasesDictJoined = use_cases_dict
    .join(dfSale, Seq("ISN_USE_CASE"), "left")
    .withColumnRenamed("name_eng", "USE_CASE_NEW")
    .join(products.select("USE_CASE_EXT_NEW", "USE_CASE_NEW", "PRODUCT_B_NEW"), Seq("USE_CASE_NEW"), "left")
    .withColumn(
      "USE_CASE_EXT_NEW",
      when(col("USE_CASE_NEW") === "TopUp", "Удержание")
        .when(col("USE_CASE_NEW") === "CPA", "Доп услуги")
        .otherwise(col("USE_CASE_EXT_NEW"))
    )
    .withColumn(
      "PRODUCT_B_NEW",
      when(col("USE_CASE_NEW") === "TopUp", "TOP UP")
        .when(col("USE_CASE_NEW") === "CPA", "CPA")
        .otherwise(col("PRODUCT_B_NEW"))
    );

  val df_FCT_AVG_CAMP_EFFECT_updated = df_FCT_AVG_CAMP_EFFECT
    .join(
      useCasesDictJoined.select("ISN_USE_CASE", "USE_CASE_EXT_NEW", "PRODUCT_B_NEW", "USE_CASE_NEW"),
      df_FCT_AVG_CAMP_EFFECT("camp_use_case_id") === use_cases_dict("ISN_USE_CASE"),
      "left"
    );

  val dd_channel = df_FCT_AVG_CAMP_EFFECT_updated
    .groupBy("name_eng")
    .agg(
      avg("monthly/sale 6mnths").alias("monthly/sale 6mnths")
    );

  val dd_big = df_FCT_AVG_CAMP_EFFECT_updated
    .groupBy("USE_CASE_EXT_NEW", "PRODUCT_B_NEW", "name_eng")
    .agg(
      avg("monthly/sale 6mnths").alias("monthly/sale 6mnths"),
      avg("ISN_USE_CASE").as("isn_use_case")
    )
    .withColumn("use_case_ext_new", trim(col("use_case_ext_new")))
    .withColumn("product_b_new", trim(col("product_b_new")))
    .filter(col("use_case_ext_new").isNotNull &&
      col("product_b_new").isNotNull &&
      col("name_eng").isNotNull)
    .drop("isn_use_case")
    .withColumnRenamed("product_b_new", "product_b_new_big")
    .withColumnRenamed("use_case_ext_new", "use_case_ext_new_big");

  val dd_product = df_FCT_AVG_CAMP_EFFECT_updated
    .groupBy("PRODUCT_B_NEW", "name_eng")
    .agg(
      avg("monthly/sale 6mnths").alias("monthly/sale 6mnths")
    )
    .filter(col("product_b_new").isNotNull &&
      col("name_eng").isNotNull);

  val updatedUseCasesDF = useCasesDictJoined
    .join(dd_big, useCasesDictJoined("PRODUCT_B_NEW") === dd_big("product_b_new_big") &&
      useCasesDictJoined("USE_CASE_EXT_NEW") === dd_big("use_case_ext_new_big"), "left")
    .withColumn("Email",
      when(
        useCasesDictJoined("Email").isNull && dd_big("name_eng") === "Email",
        dd_big("monthly/sale 6mnths")
      ).otherwise(useCasesDictJoined("Email")))
    .withColumn("Outbound",
      when(
        useCasesDictJoined("Outbound").isNull && dd_big("name_eng") === "Outbound CRM",
        dd_big("monthly/sale 6mnths")
      ).otherwise(useCasesDictJoined("Outbound")))
    .withColumn("Push",
      when(
        useCasesDictJoined("Push").isNull && dd_big("name_eng") === "Push",
        dd_big("monthly/sale 6mnths")
      ).otherwise(useCasesDictJoined("Push")))
    .withColumn("SMS",
      when(
        useCasesDictJoined("SMS").isNull && dd_big("name_eng") === "SMS Online",
        dd_big("monthly/sale 6mnths")
      ).otherwise(useCasesDictJoined("SMS")))
    .groupBy("ISN_USE_CASE", "USE_CASE_NEW", "USE_CASE_EXT_NEW", "PRODUCT_B_NEW")
    .agg(
      first(when(col("Email").isNotNull, col("Email")), ignoreNulls = true).as("Email"),
      first(when(col("Outbound").isNotNull, col("Outbound")), ignoreNulls = true).as("Outbound"),
      first(when(col("Push").isNotNull, col("Push")), ignoreNulls = true).as("Push"),
      first(when(col("SMS").isNotNull, col("SMS")), ignoreNulls = true).as("SMS")
    );

  val updatedUseCasesDict = updatedUseCasesDF
    .join(dd_product,
      Seq("product_b_new"),
      "left"
    )
    .withColumn("Email",
      when(
        col("email").isNull && col("name_eng") === "Email",
        col("monthly/sale 6mnths")
      ).otherwise(col("email"))
    )
    .withColumn("Outbound",
      when(
        col("outbound").isNull && col("name_eng") === "Outbound CRM",
        col("monthly/sale 6mnths")
      ).otherwise(col("outbound"))
    )
    .withColumn("Push",
      when(
        col("push").isNull && col("name_eng") === "Push",
        col("monthly/sale 6mnths")
      ).otherwise(col("push"))
    )
    .withColumn("SMS",
      when(
        col("sms").isNull && col("name_eng") === "SMS Online",
        col("monthly/sale 6mnths")
      ).otherwise(col("sms"))
    )
    .groupBy("ISN_USE_CASE", "USE_CASE_NEW", "USE_CASE_EXT_NEW", "PRODUCT_B_NEW")
    .agg(
      first(when(col("Email").isNotNull, col("Email")), ignoreNulls = true).as("Email"),
      first(when(col("Outbound").isNotNull, col("Outbound")), ignoreNulls = true).as("Outbound"),
      first(when(col("Push").isNotNull, col("Push")), ignoreNulls = true).as("Push"),
      first(when(col("SMS").isNotNull, col("SMS")), ignoreNulls = true).as("SMS")
    );

  val valuesMap = dd_channel.filter(col("monthly/sale 6mnths").isNotNull)
    .select("name_eng", "monthly/sale 6mnths")
    .as[(String, Double)]
    .collect()
    .toMap;

  val emailValue: Double = valuesMap.getOrElse("Email", 0.0);
  val outboundValue: Double = valuesMap.getOrElse("Outbound CRM", 0.0);
  val pushValue: Double = valuesMap.getOrElse("Push", 0.0);
  val smsValue: Double = valuesMap.getOrElse("SMS Online", 0.0);

  val updatedUseCasesDictFinal = updatedUseCasesDict
    .withColumn("Email",
      when(
        col("email").isNull,
        lit(emailValue)
      ).otherwise(col("email"))
    )
    .withColumn("Outbound",
      when(
        col("outbound").isNull,
        lit(outboundValue)
      ).otherwise(col("outbound"))
    )
    .withColumn("Push",
      when(
        col("push").isNull,
        lit(pushValue)
      ).otherwise(col("push"))
    )
    .withColumn("SMS",
      when(
        col("sms").isNull,
        lit(smsValue)
      ).otherwise(col("sms"))
    )
    .select("ISN_USE_CASE", "Email", "Outbound", "Push", "SMS");

  val pushExists = df_FCT_AVG_CAMP_EFFECT_updated.select("name_eng").distinct()
    .filter(col("name_eng") === "Push")
    .count() > 0;

  val finalDF = if (!pushExists) {
    updatedUseCasesDictFinal.withColumn("Push", col("SMS"))
      .select(
        col("ISN_USE_CASE").cast("int").as("isn_use_case"),
        col("Email"),
        col("Outbound"),
        col("Push"),
        col("SMS"))
  } else {
    updatedUseCasesDictFinal
      .select(
        col("ISN_USE_CASE").cast("int").as("isn_use_case"),
        col("Email"),
        col("Outbound"),
        col("Push"),
        col("SMS"))
  };

}
