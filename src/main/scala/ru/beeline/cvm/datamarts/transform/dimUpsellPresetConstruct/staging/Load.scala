package ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct.staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct.CustomParams

class Load extends CustomParams {

  private val socPresetShortList = List("P1D", "P1V", "P2D", "P2V", "P3D", "P3V", "P4D", "P4V", "P5D", "P5V", "P6D", "P6V")
  private val listCurrStatus: List[String] = List("A", "S")
  private val listSegmentKey: List[String] = List("N01", "N02", "N03", "N04", "N05",
    "N06", "N07", "N08", "N09", "N10",
    "N11", "N12", "AGN", "EXS", "TST")

  def getDmMobileSubscriber: DataFrame = {
    spark.read.table(TableDmMobileSubscriber)
      .filter(col("CALENDAR_DT") === LOAD_DATE_YYYY_MM_DD_2D)
      .filter(col("SUBSCRIBER_STATUS_CD").isin(listCurrStatus: _*))
      .filter(col("SEGMENT_CD").isin(listSegmentKey: _*))
      .filter(col("ACCOUNT_TYPE_CD") === 13)
      .select(
        col("SUBSCRIBER_NUM").as("ctn"),
        col("BAN_NUM").as("ban"),
        col("MARKET_CD").as("market"),
        col("PRICE_PLAN_CD").as("curr_price_plan_key")
      )
  }

  def getDimSocParameter: DataFrame = {
    spark.read.table(TableDimSocParameter)
      .na.fill(0, Array("monthly_fee", "daily_fee", "cnt_voice_local", "count_gprs"))
      .select(
        col("soc_code"),
        col("market_key").as("market_key_dim_soc"),
        when(col("monthly_fee") > 0, col("monthly_fee"))
          .otherwise(when(col("monthly_fee") === 0, col("daily_fee") * 30)).as("monthly_fee_dim_soc"),
        col("cnt_voice_local").as("minutes"),
        col("count_gprs").as("gb"))
  }

  def getAggCtnBasePubD: DataFrame = {
    spark.read.table(TableAggCtnBasePubD)
      .filter(col("time_key") === LOAD_DATE_YYYY_MM_DD_2D)
      .select(
        col("ban"),
        col("ctn"),
        coalesce(col("base"), lit(0)).as("base")
      )
  }

  def getServiceAgreementPub: DataFrame = {

    val newColumns = spark.read.table(TableServiceAgreementPub).columns
      .map(x => col(x).as(x.toLowerCase))

    val serviceAgreement = spark.read.table(TableServiceAgreementPub)
      .select(newColumns: _*)
      .na.fill(Map("p_expiration_date" -> "2999-01-01", "expiration_date" -> "2999-01-01 00:00:00"))
      .filter(col("p_expiration_date") >= lit(LOAD_DATE_YYYY_MM_DD))
      .select(
        trim(col("subscriber_no")).as("subs_key"),
        col("ban").as("ban_key"),
        trim(col("soc")).as("soc_code"),
        trim(col("service_class")).as("service_class"),
        col("expiration_date"),
        col("sys_creation_date"),
        coalesce(col("sys_update_date"), to_timestamp(col("expiration_date"))).as("sys_update_date")
      )
      .withColumn("sys_update_date_corr", greatest(col("sys_update_date"), to_timestamp(col("expiration_date"))))
      .filter(col("sys_creation_date") < lit(LOAD_DATE_YYYY_MM_DD))

    val socV = serviceAgreement
      .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
      .filter(substring(col("soc_code"), 3, 1) === "V")

    val socD = serviceAgreement
      .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
      .filter(substring(col("soc_code"), 3, 1) === "D")

    val constructorFill = spark.read.table(TableVPpConstructorParam)
      .select(col("cnt_voice_internal_local"),
              col("count_gprs"),
              col("soc_voice"),
              col("soc_gprs"))
      .distinct()

    val res = socV.as("v").join(socD.as("d"),
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
      )
    res

  }

  def getVPpConstructorParam: DataFrame = {
    val pricePlanList = List(
      "K094A17",
      "K094E17",
      "K094W26",
      "K094S26",
      "K094C26",
      "K094V26")

    spark.read.table(TableVPpConstructorParam)
      .filter(col("soc_code").isin(pricePlanList: _*))
      .select(col("market_key"),
              col("constructor_id"),
              (col("daily_fee") * 30).as("monthly_fee"),
              col("cnt_voice_internal_local").as("voice_traffic"),
              col("count_gprs").as("data_traffic"))
      .distinct()
  }

  def getStgFamilyService: DataFrame = {
    spark.read.table(TableStgFamilyService)
    .filter(col("time_key") === LOAD_DATE_YYYY_MM_DD_1D)
    .select(
      col("ctn"),
      explode(col("recipient_list")).as("recipient")
    )
  }

  def getDmMobileSubscriberUsageDaily: DataFrame = {
    spark.read.table(TableDmMobileSubscriberUsageDaily)
    .filter(col("TRANSACTION_DT").between(LOAD_DATE_YYYY_MM_DD_86D, LOAD_DATE_YYYY_MM_DD_2D))
    .filter(col("ACCOUNT_TYPE_CD") === 13)
    .filter(col("ROAMING_CD").isin("H", "X"))
    .groupBy(
      col("SUBSCRIBER_NUM").as("ctn"),
      col("BAN_NUM").as("ban"),
      col("SUBS_MARKET_CD").as("market"))
    .agg(round(
      coalesce(
        sum(when(
          col("ACTIVITY_CD") === "VOICE" &&
            col("CALL_DIRECTION_CD") === 2,
          col("usage_amt") / 60 * 1.20)),lit(0)), 2).as("voice_usage"),
       round(
         coalesce(
        sum(when(
          col("ACTIVITY_CD") === "GPRS" &&
            col("CALL_DIRECTION_CD") === 3,
          col("usage_amt") / 1024 / 1024 / 1024 * 1.20)), lit(0)), 2).as("data_usage")
           )
  }
}
