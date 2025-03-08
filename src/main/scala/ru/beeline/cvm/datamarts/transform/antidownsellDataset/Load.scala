package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

class Load extends addFunctions {

  private val lstFunction: Seq[String] = Seq("min", "max", "mean")
  private val groupColumn: Seq[String] = Seq("subs_key", "ban_key")

  def getNbaCustomer: DataFrame = {
    val maxDt = spark.read.table(TableNbaCustomers)
      .filter(col("time_key") <= SCORE_DATE)
      .select(max("time_key"))
      .first().get(0).toString

    spark.read.table(TableNbaCustomers)
      .filter(col("time_key") === maxDt)
      .filter(col("curr_subs_status_key").isin("A", "S"))
      .filter(col("account_type_key") === 13)
      .select(
        col("first_ban"),
        col("first_ctn"),
        col("last_ctn").as("subs_key"),
        col("last_ban").as("ban_key"),
        col("market_key"),
        col("price_plan_key").as("soc_code"),
        col("pp_archetype"),
        col("time_key"),
        lit(FIRST_DAY_OF_MONTH_PYYYYMM).as("month"))
  }

  def getFctRtcMonthlyPub: DataFrame = {

    val pivotTable = spark.read.table(TableFctRtcMonthlyPub)
      .filter(col("time_key").between(LOAD_DATE_PYYYYMM_7M, LOAD_DATE_PYYYYMM_2M))
      .withColumn("time_with_feature", concat_ws("_", lit(""),
        ceil(months_between(lit(FIRST_DAY_OF_MONTH_YYYY_MM_01), col("time_key_src"))),
        lit("arpu_amt_r")))
      .groupBy(col("subs_key"), col("ban_key"), col("market_key_src").as("market_key"))
      .pivot("time_with_feature")
      .agg(max(col("arpu_amt_r").cast(FloatType)))

    val statTable = spark.read.table(TableFctRtcMonthlyPub)
      .filter(col("time_key").between(LOAD_DATE_PYYYYMM_7M, LOAD_DATE_PYYYYMM_2M))
      .groupBy(col("subs_key"), col("ban_key"), col("market_key_src").as("market_key"))
      .agg(max(col("arpu_amt_r")).as("arpu_amt_r_max"),
        min(col("arpu_amt_r")).as("arpu_amt_r_min"),
        mean(col("arpu_amt_r")).as("arpu_amt_r_mean"))

    pivotTable.join(statTable, Seq("subs_key", "ban_key", "market_key"), "inner")
  }


  def getVoiceAllPub: DataFrame = {

    val featureColumns = Seq("voice_out_sec",
      "voice_in_sec",
      "voice_out_cnt",
      "voice_in_cnt",
      "voice_onnet_out_sec",
      "voice_onnet_in_sec",
      "voice_onnet_out_cnt",
      "voice_onnet_in_cnt",
      "voice_international_out_sec",
      "voice_international_in_sec",
      "voice_international_out_cnt",
      "voice_international_in_cnt",
      "voice_intercity_out_sec",
      "voice_intercity_in_sec",
      "voice_intercity_out_cnt",
      "voice_intercity_in_cnt",
      "voice_out_nopackage_sec")

    val aggFeature = featureColumns
      .flatMap(x => x match {
        case "voice_in_sec" => Seq(sum(col(x)).as(x), count(col(x)).as("voice_in_day_usage_cnt"))
        case "voice_out_sec" => Seq(sum(col(x)).as(x), count(col(x)).as("voice_out_day_usage_cnt"))
        case _ => Seq(sum(col(x)).as(x))
      })

    val srcTable = spark.read.table(TableVoiceAllPub)
      .filter(col("time_key").between(FIRST_DAY_OF_MONTH_YYYY_MM_01_6M, LAST_DAY_OF_MONTH_YYYY_MM_DD_1M))
      .withColumn("tk", trunc(col("time_key"), "month"))
      .groupBy(col("subs_key"), col("ban_key"), col("tk"))
      .agg(aggFeature.head, aggFeature.tail: _*)

    val newFeature = featureColumns :+ "voice_in_day_usage_cnt" :+ "voice_out_day_usage_cnt"
    val statTable = featureCollect(srcTable, lstFunction, newFeature, groupColumn)
    val pivotTable = pivotsTable(srcTable, "tk", newFeature, groupColumn)

    statTable.join(pivotTable, Seq("subs_key", "ban_key"), "inner")
  }

  def getDataAllPub: DataFrame = {
    val featureColumns = Seq("data_all_mb", "data_4g_mb")
    val aggFeature = featureColumns
      .flatMap(x => x match {
        case "data_all_mb" => Seq(sum(col(x)).as(x), count(col(x)).as("data_all_mb_day_usage_cnt"))
        case _ => Seq(sum(col(x)).as(x))
      })

    val srcTable = spark.read.table(TableDataAllPub)
      .filter(col("time_key").between(FIRST_DAY_OF_MONTH_YYYY_MM_01_6M, LAST_DAY_OF_MONTH_YYYY_MM_DD_1M))
      .withColumn("tk", trunc(col("time_key"), "month"))
      .groupBy(col("subs_key"), col("ban_key"), col("tk"))
      .agg(aggFeature.head, aggFeature.tail: _*)

    val newFeature = featureColumns :+ "data_all_mb_day_usage_cnt"
    val statTable = featureCollect(srcTable, lstFunction, newFeature, groupColumn)
    val pivotTable = pivotsTable(srcTable, "tk", newFeature, groupColumn)

    statTable.join(pivotTable, Seq("subs_key", "ban_key"), "inner")
  }

  def getFamilyService: DataFrame = {
    val featureColumns = Seq("cnt_recipients")
    val aggFeature = featureColumns.map(x => max(x).as(x))

    val srcTable = spark.read.table(TableFamilyService)
      .filter(col("time_key").between(FIRST_DAY_OF_MONTH_YYYY_MM_01_6M, LAST_DAY_OF_MONTH_YYYY_MM_DD_1M))
      .withColumn("tk", trunc(col("time_key"), "month"))
      .groupBy(col("ctn").as("subs_key"), col("tk"))
      .agg(aggFeature.head, aggFeature.tail: _*)

    val statTable = featureCollect(srcTable, lstFunction, featureColumns, Seq("subs_key"))
    val pivotTable = pivotsTable(
      srcTable.filter(col("tk") === FIRST_DAY_OF_MONTH_YYYY_MM_01_2M), "tk", featureColumns, Seq("subs_key"), true)

    statTable.join(pivotTable, Seq("subs_key"), "inner")
  }

  def getFctActClustSubsMPub: DataFrame = {

    val featureColumns = Seq("umcs_rev", "other_revenue", "ics_rev", "direct_rev", "total_rev", "data_h_rev", "oc_rev",
      "service_margin", "contribution_margin", "contact_bln", "contact_mts", "contact_mgf", "contact_tl2",
      "total_contact", "permanent_aab", "phone_name_max_traf", "bs_cnt_total",
      "total_recharge_amt_rur", "balance_amt_end", "cnt_recharge")

    val srcTable = spark.read.table(TableFctActClustSubsMPub)
      .filter(col("time_key").between(LOAD_DATE_PYYYYMM_7M, LOAD_DATE_PYYYYMM_2M))
      .withColumnRenamed("last_subs_key", "subs_key")
      .withColumnRenamed("last_ban_key", "ban_key")

    val pvTable = srcTable
      .filter(col("time_key") === LOAD_DATE_PYYYYMM_2M)
      .withColumn("tk", trunc(col("time_key_src"), "month"))

    val statTable = featureCollect(srcTable, lstFunction, featureColumns, groupColumn)
    val pivotTable = pivotsTable(pvTable, "tk", featureColumns, groupColumn)

    statTable.join(pivotTable, Seq("subs_key", "ban_key"), "inner")
  }

  def getDimBan: DataFrame = {
    spark.read.table(TableDimBanPub)
      .select(
        col("ban_key"),
        col("customer_gender"),
        col("market_key_src").as("market_key"),
        round(datediff(lit(SCORE_DATE), col("customer_birth_date")) / 365.25).as("customer_age"))
  }

  def getDimSocParameter: DataFrame = {
    spark.read.table(TableDimSocParameter)
      .filter(col("soc_code").isNotNull && col("product_id").isNotNull)
      .select("soc_code", "product_id", "market_key", "business_name", "monthly_fee", "daily_fee", "arhive_ind",
        "cnt_voice_internal_local", "cnt_voice_local", "count_gprs", "count_sms", "count_availabl_addl_ctn_family")
  }

  def getDimDic: DataFrame = spark.read.table(TableDimDic).select("isn", "name_eng", "parent_isn")

  def getDimDicLink: DataFrame = spark.read.table(TableDimDicLink).select("dic1_isn", "dic2_isn")

  def getDimPpConstructorSoc: DataFrame = {
    val maxDt = spark.read.table(TableDimPpConstructorSoc).select(max("report_dt"))
      .first().get(0).toString

    val fillConstructor = spark.read.table(TableDimPpConstructorSoc)
      .filter(col("report_dt") === maxDt)
      .select(col("price_plan").as("soc_code"), col("market_key"), col("constructor_id"),
        col("soc"), col("unit"), col("active_ind"), col("class"), col("daily_rc"),
        col("long_rc"), col("wep_pkg_amount_long"), col("start_time_key"), col("end_time_key"))
      .distinct()

    fillConstructor
  }


  def getInitActivationAndCoreAABDate: DataFrame = {
    spark.read.table(TableFctActClustSubsMPub)
      .filter(col("time_key") === LOAD_DATE_PYYYYMM_2M)
      .withColumnRenamed("last_subs_key", "subs_key")
      .withColumnRenamed("last_ban_key", "ban_key")
      .select("subs_key", "ban_key", "core_aab_first_month", "init_activation_date")
  }

  def getServiceAgreementPub: DataFrame = {
    val newColumns = spark.read.table(TableServiceAgreementPub).columns
      .map(x => col(x).as(x.toLowerCase))

    spark.read.table(TableServiceAgreementPub)
      .select(newColumns: _*)
      .na.fill(Map("p_expiration_date" -> "2999-01-01", "expiration_date" -> "2999-01-01 00:00:00"))
      .filter(col("p_expiration_date") >= lit(FIRST_DAY_OF_MONTH_YYYY_MM_01))
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
      .filter(col("sys_creation_date") < lit(SCORE_DATE_1))
  }
  def getDimSubscriberPub: DataFrame = {
    val newColumns = spark.read.table(TableDimSubscriberPub).columns
      .map(x => col(x).as(x.toLowerCase))

    spark.read.table(TableDimSubscriberPub)
      .select(newColumns: _*)
      .filter(col("curr_dw_status").isin("A", "S"))
      .filter(col("account_type_key") === 13)
      .select(
        col("ban_key"),
        col("subs_key"),
        col("market_key_src").as("market_key"),
        col("curr_price_plan_key").as("soc_code"),
        col("price_plan_change_date"))

  }
}
