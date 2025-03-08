package ru.beeline.cvm.datamarts.transform.fctCampEffects

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, add_months, avg, col, current_date, date_format, desc, first, lit, lower, max, mean, row_number, sum, trim, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}

class FctDetailCampEffect extends Load {

  private def condition(colName: String) = col(colName).isNull || abs(col(colName)) === Double.PositiveInfinity

  private def lowerAndTrim(df: DataFrame, cols: Seq[String]): DataFrame = {
    cols.foldLeft(df)((tempDf, colName) =>
      tempDf.withColumn(colName, trim(lower(col(colName))))
    )
  }

  val campLibrary = getCampLibrary
  val wave = getWave
  val campToEventType = getCampToEventType
  val cumulativeEffCampaign = getCumulativeEffCampaing
  val tCampsLastPart = getTCampsLastPart
  val dimDict = getDimDic
  val avgSaleUseCase = getFctAvgSaleIsnUseCase

  val sdf = campLibrary
    .join(
      wave
        .where(col("wave_month").cast("date") === date_format(add_months(lit(SCORE_DATE_PLUS_1_MONTH), 0), "yyyy-MM-01"))
        .where(col("wave_type_id") === 45),
      Seq("wave_id"), "inner"
    )
    .join(
      dimDict
        .select(
          col("isn").alias("camp_use_case_id"),
          col("name_eng").alias("use_case")),
      Seq("camp_use_case_id"), "left"
    )
    .join(campToEventType, Seq("camp_id"), "left")
    .where(col("event_type_id") === "11728")
    .select(
      col("camp_id"),
      col("CAMP_NAME_SHORT").as("camp_name_short"),
      col("PARENT_CAMP_CODE").as("parent_camp_code"),
      col("wave_month"),
      col("info_channel_id"),
      col("wave_id"),
      col("camp_use_case_id"),
      col("use_case")
    );

  val preEffects = cumulativeEffCampaign
    .withColumn(
      "sum_0_2",
      sum(col("effect_arpu") / col("tg_size")).over(Window.partitionBy("camp_id").orderBy("diff").rowsBetween(Window.currentRow, 2))
    )
    .withColumn(
      "monthly_effect",
      sum(col("effect_arpu") / (col("tg_size") * 3)).over(Window.partitionBy("camp_id").orderBy("diff").rowsBetween(Window.currentRow, 2))
    )
    .select("camp_id", "wave_month", "time_key", "tg_size", "sum_0_2", "monthly_effect")
    .join(campLibrary, Seq("camp_id"), "left")
    .join(tCampsLastPart, Seq("camp_id"), "left")
    .join(
      dimDict
        .groupBy("name_eng")
        .agg(max("isn").alias("isn"))
        .select(col("isn"), col("name_eng").alias("USE_CASE_NEW")),
      Seq("USE_CASE_NEW"), "left"
    )
    .where(cumulativeEffCampaign("time_key") === cumulativeEffCampaign("wave_month").cast("date"))
    .where(
      cumulativeEffCampaign("wave_month").cast("date") >= date_format(add_months(current_date(), -13), "yyyy-MM-01")
    )
    .where(
      tCampsLastPart("info_channel_name").isin("SMS Online", "Email", "Outbound CRM", "Push")
    )
    .select(
      cumulativeEffCampaign("camp_id").alias("camp_id"),
      cumulativeEffCampaign("wave_month"),
      cumulativeEffCampaign("time_key"),
      cumulativeEffCampaign("tg_size"),
      tCampsLastPart("info_channel_name"),
      col("sum_0_2"),
      col("monthly_effect"),
      col("camp_use_case_id"),
      campLibrary("parent_camp_code"),
      col("use_case_ext_new"),
      col("product_b_new"),
      tCampsLastPart("USE_CASE_NEW"),
      col("isn").alias("isn"),
      col("tg_sale"),
      (col("tg_sale") / cumulativeEffCampaign("tg_size")).as("sale_fact")
    )
    .filter(col("tg_size") >= 1000);

  val effects = lowerAndTrim(preEffects, Seq("use_case_new", "product_b_new", "use_case_ext_new", "info_channel_name"));

  val dfSales = avgSaleUseCase
    .selectExpr("isn_use_case", "stack(" +
      (avgSaleUseCase.columns.length - 1) + ", " +
      avgSaleUseCase.columns.filter(_ != "isn_use_case").map(c => s"'$c', $c").mkString(", ") +
      ") as (channel_name, `sale,%`)")
    .withColumn(
      "channel_name",
      when(col("channel_name") === "sms", "sms online")
        .when(col("channel_name") === "outbound", "outbound crm")
        .otherwise(col("channel_name"))
    );

  val dfEffects = effects
    .join(dfSales, effects("isn") === dfSales("isn_use_case") && effects("info_channel_name") === dfSales("channel_name"), "left")
    .withColumn("monthly/sale", lit(null))
    .withColumn(
      "monthly/sale",
      when(col("sale_fact") === 0, col("monthly_effect") / col("sale,%"))
        .otherwise(col("monthly_effect") / col("sale_fact"))
    );

  val dfEffectParentCode = dfEffects.filter(col("parent_camp_code").isNotNull);

  val windowSpec = Window.partitionBy("parent_camp_code").orderBy(desc("wave_month"));

  val rankedDf = dfEffectParentCode.withColumn("row_num", row_number().over(windowSpec));

  val filteredDf = rankedDf.filter(col("row_num") <= 3)
    .groupBy("parent_camp_code")
    .agg(
      avg("monthly/sale").as("monthly/sale")
    );

  val aggregatedDf = dfEffectParentCode
    .groupBy("parent_camp_code")
    .agg(
      first("use_case_new").as("use_case_new"),
      first("product_b_new").as("product_b_new"),
      first("use_case_ext_new").as("use_case_ext_new"),
      first("info_channel_name").as("info_channel_name")
    );

  val dfResult = aggregatedDf.join(filteredDf, Seq("parent_camp_code"), "left");

  val dfWhole = sdf
    .join(dfResult, Seq("parent_camp_code"), "left")
    .withColumn(
      "info_channel_name",
      when(col("info_channel_name").isNull && col("info_channel_id") === 11156, "sms online")
        .when(col("info_channel_name").isNull && col("info_channel_id") === 11157, "email")
        .when(col("info_channel_name").isNull && col("info_channel_id") === 19275, "push")
        .when(col("info_channel_name").isNull && col("info_channel_id") === 12371, "outbound crm")
        .otherwise(col("info_channel_name"))
    );

  val ddBig = dfEffectParentCode
    .groupBy("info_channel_name", "product_b_new", "use_case_ext_new")
    .agg(
      (sum(col("monthly/sale") * col("tg_size")) / sum(col("tg_size"))).alias("weighted_average")
    )
    .filter(col("product_b_new").isNotNull && col("use_case_ext_new").isNotNull)
    .filter(col("weighted_average") =!= 0);

  val ddProduct = dfEffectParentCode
    .groupBy("info_channel_name", "product_b_new")
    .agg(
      (sum(col("monthly/sale") * col("tg_size")) / sum(col("tg_size"))).alias("weighted_average")
    )
    .filter(col("product_b_new").isNotNull)
    .filter(col("weighted_average") =!= 0);

  val ddChannel = ddProduct
    .groupBy("info_channel_name")
    .agg(mean("weighted_average").as("weighted_average"));

  val dd_big_renamed = ddBig
    .withColumnRenamed("product_b_new", "dd_big_product_b_new")
    .withColumnRenamed("use_case_ext_new", "dd_big_use_case_ext_new")
    .withColumnRenamed("info_channel_name", "dd_big_info_channel_name")
    .withColumnRenamed("weighted_average", "dd_big_weighted_average");

  val df_result_step1 = dfWhole
    .join(dd_big_renamed,
      col("product_b_new") === col("dd_big_product_b_new") &&
        col("use_case_ext_new") === col("dd_big_use_case_ext_new") &&
        col("info_channel_name") === col("dd_big_info_channel_name"),
      "left"
    )
    .withColumn(
      "monthly/sale",
      when(condition("monthly/sale"), col("dd_big_weighted_average"))
        .otherwise(col("monthly/sale"))
    )
    .drop("dd_big_product_b_new", "dd_big_use_case_ext_new", "dd_big_info_channel_name", "dd_big_weighted_average");

  val dd_product_renamed = ddProduct
    .withColumnRenamed("product_b_new", "dd_product_product_b_new")
    .withColumnRenamed("info_channel_name", "dd_product_info_channel_name")
    .withColumnRenamed("weighted_average", "dd_product_weighted_average");

  val df_result_step2 = df_result_step1
    .join(dd_product_renamed,
      col("product_b_new") === col("dd_product_product_b_new") &&
        col("info_channel_name") === col("dd_product_info_channel_name"),
      "left"
    )
    .withColumn(
      "monthly/sale",
      when(condition("monthly/sale"), col("dd_product_weighted_average"))
        .otherwise(col("monthly/sale"))
    )
    .drop("dd_product_product_b_new", "dd_product_info_channel_name", "dd_product_weighted_average");

  val dd_channel_renamed = ddChannel
    .withColumnRenamed("info_channel_name", "dd_channel_info_channel_name")
    .withColumnRenamed("weighted_average", "dd_channel_weighted_average");

  val df_result_step3 = df_result_step2
    .join(dd_channel_renamed,
      col("info_channel_name") === col("dd_channel_info_channel_name"),
      "left"
    )
    .withColumn(
      "monthly/sale",
      when(condition("monthly/sale"), col("dd_channel_weighted_average"))
        .otherwise(col("monthly/sale"))
    )
    .drop("dd_channel_info_channel_name", "dd_channel_weighted_average");

  val df_result_with_sale_6mnths = df_result_step3.withColumn("monthly/sale 6mnths", col("monthly/sale") * 6);

  val df_FCT_DETAIL_CAMP_EFFECT_to_hue = df_result_with_sale_6mnths
    .select("camp_id", "parent_camp_code", "wave_id", "camp_use_case_id", "monthly/sale 6mnths")
    .dropDuplicates();

  val renamed_df = df_FCT_DETAIL_CAMP_EFFECT_to_hue
    .withColumnRenamed("camp_use_case_id", "ISN_USE_CASE")
    .withColumnRenamed("monthly/sale 6mnths", "VALUE");

  val sdf_detail_new = renamed_df
    .withColumn("camp_id", col("camp_id").cast(IntegerType))
    .withColumn("parent_camp_code", col("parent_camp_code").cast(StringType))
    .withColumn("wave_id", col("wave_id").cast(IntegerType))
    .withColumn("ISN_USE_CASE", col("ISN_USE_CASE").cast(IntegerType))
    .withColumn("VALUE", col("VALUE").cast(DoubleType));

}
