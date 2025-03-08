package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

object CoreClientsRecharges extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val recharge = spark.read.table(tableRechargesPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("recharge_amt").alias("recharge_amt_daily")
    )

  val rechargeArray = dayInflow
    .join(recharge, dayInflow("subs_key")===recharge("first_ctn")&&
      dayInflow("ban_key")===recharge("first_ban")&&
      dayInflow("data") === recharge("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(max(col("recharge_amt_daily")).as("recharge_amt_daily"))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"$i")): _*).as("recharge_amt_daily")
    )

  val rechargeFeatures = dayInflow
    .join(recharge, dayInflow("subs_key")===recharge("first_ctn")&&
      dayInflow("ban_key")===recharge("first_ban")&&
      dayInflow("data") === recharge("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("recharge_amt_daily")).alias("recharge_amt_daily_max"),
      sum(col("recharge_amt_daily")).alias("recharge_amt_daily_sum"),
      avg(col("recharge_amt_daily")).alias("recharge_amt_daily_avg")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("recharge_amt_daily_max"),
      col("recharge_amt_daily_sum"),
      col("recharge_amt_daily_avg")
    )

  val core_clients_sample_recharges = rechargeArray
    .join(rechargeFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_recharges
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_recharges")

}
