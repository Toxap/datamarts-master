package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions.{col, substring, to_date, when}

object InflowDaily extends CustomParams{
  val inflow = spark.read.table(tableFctSubsInflowDailyPub)
    .where(col("inflow_ind") === 1)
    .where(col("long_idle_ind") === 0)
    .withColumn("sale_dt", substring(col("date_key_src"), 1, 10))
    .filter(col("sale_dt") === LOAD_DATE_YYYY_MM_DD_14)
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("account_type_key"),
      col("date_key_src"),
      col("channel_key")
    )

  val account = spark.read.table(tableDimAccountType)
    .where(col("business_type") === "B2C")
    .select(
      col("account_type_code"),
      col("business_type")
    )

  val channelHist = spark.read.table(tableMvChannelHistPub)
    .filter(col("leading_ban") === "Y" || col("leading_ban") === "-99")
    .select(
      col("channel_key"),
      col("effective_date"),
      col("expiration_date"),
      col("dealer_group_key")
    )

  val dicMapElem = spark.read.table(tableDmDicMapElemVerHistVPub)
    .filter(col("map_id") === "SALES_CH_BIIS_H")
    .withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd"))
    .withColumn("expiration_date", to_date(col("expiration_date"), "yyyy-MM-dd"))
    .select(
      col("src1_elem_id"),
      col("effective_date"),
      col("expiration_date"),
      col("tgt_elem_id")
    )

  val hyMgkpi = spark.read.table(tableDimHyMgkpiDimPub)
    .filter(col("dimension") === "SalesChannels")
    .filter(col("data_storage") =!= "Shared")
    .select("id", "effective_month", "expiration_month", "eng_alias")

  val base = inflow
    .join(account, inflow("account_type_key") === account("account_type_code"), "left")
    .join(channelHist, inflow("channel_key") === channelHist("channel_key") &&
      inflow("date_key_src") >= channelHist("effective_date") &&
      inflow("date_key_src") < channelHist("expiration_date"), "left")
    .join(dicMapElem, channelHist("dealer_group_key") === dicMapElem("src1_elem_id") &&
      dicMapElem("effective_date") <= inflow("date_key_src") &&
      dicMapElem("expiration_date") > inflow("date_key_src"), "left")
    .join(hyMgkpi, dicMapElem("tgt_elem_id") === hyMgkpi("id") &&
      hyMgkpi("effective_month") <= inflow("date_key_src") &&
      hyMgkpi("expiration_month") > inflow("date_key_src"), "left")
    .withColumn("flag_monobrand", when(col("eng_alias") === "Own Offices", 1).otherwise(0))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("flag_monobrand")
    )

  base
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_base")

}
