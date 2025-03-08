package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

object CoreClientsCharges extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val charge = spark.read.table(tableCommArpuAllPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("charge_amt").alias("charge_amt_daily")
    )

  val chargeArray = dayInflow
    .join(charge, dayInflow("subs_key")===charge("first_ctn")&&
      dayInflow("ban_key")===charge("first_ban")&&
      dayInflow("data") === charge("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(max(col("charge_amt_daily")).as("charge_amt_daily"))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"$i")): _*).as("charge_amt_daily")
    )

  val chargeFeatures = dayInflow
    .join(charge, dayInflow("subs_key")===charge("first_ctn")&&
      dayInflow("ban_key")===charge("first_ban")&&
      dayInflow("data") === charge("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("charge_amt_daily")).alias("charge_amt_daily_max"),
      sum(col("charge_amt_daily")).alias("charge_amt_daily_sum"),
      avg(col("charge_amt_daily")).alias("charge_amt_daily_avg")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("charge_amt_daily_max"),
      col("charge_amt_daily_sum"),
      col("charge_amt_daily_avg")
    )

  val core_clients_sample_charge = chargeArray
    .join(chargeFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_charge
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_charges")

}
