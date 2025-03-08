package ru.beeline.cvm.datamarts.transform.commPolit

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{add_months, col, date_add, from_unixtime, lit, to_date, when}

class Load extends addFunction {

  def getBase: DataFrame = {

    val listCurrStatus: List[String] = List("A", "S")

    spark.read.table(TableDmMobileSubscriber)
      .filter(col("CALENDAR_DT") === SCORE_DATE_4)
      .filter(col("SUBSCRIBER_STATUS_CD").isin(listCurrStatus: _*))
      .filter(col("ACCOUNT_TYPE_CD") === 13)
      .withColumn("time_key_src", lit(SCORE_DATE))
      .select(
        col("SUBSCRIBER_NUM").as("subs_key"),
        col("BAN_NUM").as("ban_key"),
        col("MARKET_CD").as("market_key"),
        col("time_key_src")
      )
      .distinct()

  }

  def clust: DataFrame = {

    val clust = spark.table(TableFctActClustSubsMPub)
      .drop("time_key_src")
      .filter(col("time_key").between(LOAD_DATE_PYYYYMM_6M, LOAD_DATE_PYYYYMM_2M))
      .withColumn("bs_data_rate", when(col("bs_cnt_total") > 0, when((col("bs_cnt_data") / col("bs_cnt_total")).isNull, 0)
        .otherwise(col("bs_cnt_data") / col("bs_cnt_total")))
        .otherwise(0))
      .withColumn("bs_voice_rate", when(col("bs_cnt_total") > 0, when((col("bs_cnt_voice") / col("bs_cnt_total")).isNull, 0)
        .otherwise(col("bs_cnt_voice") / col("bs_cnt_total"))).otherwise(0))
      .withColumn("is_smartphone", when(col("device_type").isin("A", "S"), 1)
        .otherwise(0)).withColumnRenamed("time_key", "clust_time_key")
      .withColumn("in_voice", lit(in_voice))
      .withColumn("out_voice", lit(out_voice))

    clust
  }

  def getDmMobileSubscriber: DataFrame = {
    spark.table(TableDmMobileSubscriber)
      .filter(col("calendar_dt") === SCORE_DATE_4)
      .select(
        col("subscriber_num").as("subs_key"),
        col("ban_num").as("ban_key"),
        col("market_cd").as("market_key")
      )
      .distinct()
  }


  def getTCamps: DataFrame = {
    spark.table(TableTCampsLastPart)
  }


  def getCmsMembersClear: DataFrame = {
    spark.table(TableCmsMembers).as("m")
      .filter(col("time_key") >= FIRST_DAY_OF_MONTH_YYYY_MM_01_12M)
      .filter(col("time_key") <= FIRST_DAY_OF_MONTH_YYYY_MM_01)
      .join(getDmMobileSubscriber.as("dM"), Seq("subs_key", "ban_key", "market_key"), "inner")
      .join((getTCamps
        .filter(col("ussd_ind") === 0)
        .filter(!col("info_channel_name").like("%nbound%"))).as("t"), Seq("camp_id"), "inner")
      .withColumn("flist", lit(1))
      .withColumn("sale_ind", when(col("soc_date_on").isNotNull, 1))
      .select(
        col("subs_key"),
        col("ban_key"),
        col("market_key"),
        col("flist"),
        col("real_taker_ind"),
        col("m.time_key_src").as("time_key"),
        col("response_date"),
        col("m.soc_list"),
        col("m.soc_disc"),
        col("m.camp_id"),
        col("t.wave_month"),
        col("response_class"),
        col("sale_ind"),
        from_unixtime(col("time_key_src") / 1000 + 1 / 8).as("d0"),
        from_unixtime(col("time_key_src") / 1000 + 1 / 8).as("d5"),
        date_add(from_unixtime(col("time_key_src") / 1000 + 1 / 8), 31).as("d30"),
        date_add(from_unixtime(col("time_key_src") / 1000 + 1 / 8), 91).as("d90"),
        date_add(from_unixtime(col("time_key_src") / 1000 + 1 / 8), 182).as("d180"),
        date_add(from_unixtime(col("time_key_src") / 1000 + 1 / 8), 366).as("d365")
      )

  }

  def getAggOutboundDetailHistory: DataFrame = {
    spark.table(TableAggOutboundDetailHistory)
      .withColumn("utk", lit(SCORE_DATE))
      .filter(col("response_date_src") >= add_months(col("utk"), -12));
  }


  def getTCampsFilters: DataFrame = {
    spark.table(TableTCampsLastPart)
      .filter(col("ussd_ind") === 0)
      .filter(col("technical_wevent_b2b_test") === 0)
      .filter(col("use_case_ext_new") === "Апгрейд тарифа")
      .filter(col("info_channel_name") === "Outbound CRM");
  }
}
