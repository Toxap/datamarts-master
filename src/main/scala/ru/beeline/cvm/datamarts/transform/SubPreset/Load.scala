package ru.beeline.cvm.datamarts.transform.SubPreset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, expr, greatest, least, lit, max, substring, to_timestamp, trim, when, explode}

class Load extends CustomParams {

  val listPricePlanKey = Seq(
    "K094A12",
    "K094E12",
    "K094W21",
    "K094S21",
    "K094C21",
    "K094V21");

  val marketKeyEast = Seq(
    "MGD", "OMS", "BIR", "GAL", "ABK", "ANR",
    "NOR", "NSK", "BGK", "YAK", "KRS", "KSK", "VLA", "IRK", "TMS",
    "BUR", "SHL", "CHT", "DTI", "USH", "KMR", "KZL", "SKH", "BAR", "PPK", "HBR"
  );

  def getDimSoc: DataFrame = {
    spark.read.table(TableDimSocParameter)
      .filter(
        col("soc_code").isNotNull &&
          col("product_id").isNotNull)
      .select(
        col("soc_code"),
        col("market_key"),
        col("business_name"),
        col("monthly_fee"),
        col("daily_fee"),
        col("cnt_voice_internal_local").as("current_minutes"),
        col("count_gprs").as("current_gb"),
        col("count_sms"))
  }

  def getDmSubs: DataFrame = {
    spark.read.table(TableDmMobileSubscriber)
      .filter(col("CALENDAR_DT") === SCORE_DATE_4)
      .filter(col("ACCOUNT_TYPE_CD").isin(13, 341))
      .filter(col("SUBSCRIBER_STATUS_CD").isin("A", "S"))
      .select(
        col("SUBSCRIBER_NUM"),
        col("BAN_NUM"))
  }

  def getSubsSoc(dimSoc: DataFrame): DataFrame = {
    spark.read.table(TableDmMobileSubscriber)
      .filter(col("CALENDAR_DT") === SCORE_DATE_4)
      .filter(col("ACCOUNT_TYPE_CD").isin(13, 341))
      .filter(col("SUBSCRIBER_STATUS_CD").isin("A", "S"))
      .select(
        col("SUBSCRIBER_NUM").as("ctn"),
        col("BAN_NUM").as("ban"),
        col("MARKET_CD").as("market_key"),
        col("PRICE_PLAN_CD").as("soc_code"))
      .join(dimSoc, Seq("soc_code", "market_key"), "left")
      .withColumnRenamed("soc_code", "org_soc_code")
  }

  def getPpParam(listPricePlanKey: Seq[String]): DataFrame = {
    spark.read.table(TableVPpConstructorParam)
      .filter(col("soc_code").isin(listPricePlanKey: _*))
      .withColumnRenamed("daily_fee", "constructor_daily_fee")
      .withColumnRenamed("monthly_fee", "constructor_monthly_fee")
      .withColumnRenamed("soc_code", "price_plan")
      .filter(col("count_gprs") <= 50)
      .filter(!col("market_key").isin(marketKeyEast: _*))
      .filter(col("constructor_id") =!= 1)
      .filter(col("count_gprs") =!= 0)
      .filter(col("cnt_voice_internal_local") =!= 0)
      .select(
        "price_plan",
        "market_key",
        "soc_desc",
        "cnt_voice_internal_local",
        "count_gprs",
        "soc_voice",
        "soc_gprs",
        "constructor_id",
        "constructor_daily_fee",
        "constructor_monthly_fee")
  }

  def getPpParamEast(listPricePlanKey: Seq[String]): DataFrame = {
    spark.read.table(TableVPpConstructorParam)
      .filter(col("soc_code").isin(listPricePlanKey: _*))
      .withColumnRenamed("daily_fee", "constructor_daily_fee")
      .withColumnRenamed("monthly_fee", "constructor_monthly_fee")
      .withColumnRenamed("soc_code", "price_plan")
      .filter(col("count_gprs") <= 50)
      .filter(col("market_key").isin(marketKeyEast: _*))
      .select(
        "price_plan",
        "market_key",
        "soc_desc",
        "cnt_voice_internal_local",
        "count_gprs",
        "soc_voice",
        "soc_gprs",
        "constructor_id",
        "constructor_daily_fee",
        "constructor_monthly_fee")
  }

  def getSocUpFill: DataFrame = {
    val socPresetShortList = List("P1D", "P1V", "P2D", "P2V", "P3D", "P3V", "P4D", "P4V", "P5D", "P5V", "P6D", "P6V");

    val newColumns = spark.read.table(TableServiceAgreementPub).columns
      .map(x => col(x).as(x.toLowerCase));

    val serviceAgreement = spark.read.table(TableServiceAgreementPub)
      .select(newColumns: _*)
      .na.fill(Map("p_expiration_date" -> "2999-01-01", "expiration_date" -> "2999-01-01 00:00:00"))
      .filter(col("p_expiration_date") >= lit(SCORE_DATE))
      .select(
        trim(col("subscriber_no")).as("subs_key"),
        col("ban").as("ban_key"),
        trim(col("soc")).as("soc_code"),
        trim(col("service_class")).as("service_class"),
        col("expiration_date"),
        col("sys_creation_date"),
        coalesce(col("sys_update_date"), to_timestamp(col("expiration_date"))).as("sys_update_date")
      )
      .withColumn("sys_update_date_corr", greatest(col("sys_update_date"), to_timestamp(col("expiration_date"))));

    val socV = serviceAgreement
      .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
      .filter(substring(col("soc_code"), 3, 1) === "V");

    val socD = serviceAgreement
      .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
      .filter(substring(col("soc_code"), 3, 1) === "D");

    val constructorFill = spark.read.table(TableVPpConstructorParam)
      .select(col("cnt_voice_internal_local"),
        col("count_gprs"),
        col("soc_voice"),
        col("soc_gprs"))
      .distinct();

    val voiceDataFillUp = socV.as("v").join(socD.as("d"),
        col("v.subs_key") === col("d.subs_key") && col("v.ban_key") === col("d.ban_key"), "inner")
      .select(
        col("v.subs_key"),
        col("v.ban_key"),
        greatest(col("v.sys_creation_date"), col("d.sys_creation_date")).as("sys_creation_date"),
        greatest(col("v.sys_update_date_corr"), col("d.sys_update_date_corr")).as("sys_update_date_corr"),
        least(col("v.expiration_date"), col("d.expiration_date")).as("expiration_date"),
        col("v.soc_code").as("soc_voice"),
        col("d.soc_code").as("soc_gprs"))
      .join(constructorFill, Seq("soc_voice", "soc_gprs"))
      .select(
        col("ban_key").as("ban"),
        col("subs_key").as("ctn"),
        col("cnt_voice_internal_local").as("curr_voice_fill"),
        col("count_gprs").as("curr_data_fill")
      );

    voiceDataFillUp
  }

  def getBase: DataFrame = {
    spark.read.table(TableAggCtnBasePubD)
      .filter(col("time_key") === SCORE_DATE_3)
      .select("ctn", "ban", "base")
  }

  def getSegment: DataFrame = {
    val lastPart = spark.table("ca_b2c.ca_main_subscriber")
      .select(max(col("transaction_dt")))
      .collect()(0)(0)

    spark.table("ca_b2c.ca_main_subscriber")
      .filter(col("transaction_dt") === lastPart)
      .withColumn("social_segment",
        when(col("social_segment") === "Семья",
          lit("Constr_Up_Bee_04.2024"))
          .when(col("social_segment") === "МВС", lit("Constr_Up_Cat_04.2024"))
          .when(col("social_segment") === "Социальный", lit("Constr_Up_Panda_04.2024"))
          .when(col("social_segment") === "Мигранты", lit("Constr_Up_Robot_04.2024"))
          .otherwise(lit("Constr_Up_Dragon_04.2024")))
      .select(
        col("subscriber_num").as("ctn"),
        col("ban_num").as("ban"),
        col("social_segment").as("soc_desc")
      );
  }

  def getFamily: DataFrame = {
    spark.table("cm_datamarts.stg_family_service")
      .filter(col("time_key") === SCORE_DATE_2)
      .filter(col("cnt_recipients").isNotNull && col("cnt_recipients") > 0)
      .withColumn("recipient", explode(col("recipient_list")));
  }


}
