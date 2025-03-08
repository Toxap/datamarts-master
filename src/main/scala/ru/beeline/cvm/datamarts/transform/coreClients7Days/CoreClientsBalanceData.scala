package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CoreClientsBalanceData extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val customers_data = spark.read.table(tableCustomersPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumn("duplicate", count("lifetime").over(Window.partitionBy("first_ban", "first_ctn", "time_key")))
    .filter(col("duplicate") === 1)
    .withColumnRenamed("time_key", "cust_time_key")
    .select(
      col("first_ctn"),
      col("first_ban"),
      col("last_ctn"),
      col("last_ban"),
      col("cust_time_key")
    )

  val base_tmp = dayInflow
    .join(customers_data, dayInflow("subs_key") === customers_data("first_ctn") &&
      dayInflow("ban_key") === customers_data("first_ban") &&
      dayInflow("data") === customers_data("cust_time_key"), "left")
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("last_ctn"),
      col("last_ban"),
      col("data"),
      col("day")
    )

  val balance = spark.read.table(tableAggCtnBalancePubD)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("ctn"),
      col("ban"),
      col("balance").alias("balance_daily"),
      col("bal_neg_flg").alias("bal_neg_flg_daily"),
      col("bal_low_flg").alias("bal_low_flg_daily"),
      col("bal_high_flg").alias("bal_high_flg_daily"),
      col("bal_is_enough").alias("bal_is_enough_daily"),
      col("bal_delta").alias("bal_delta_daily"),
      col("days_cnt_last").alias("days_cnt_last_daily"),
      col("days_cnt_next").alias("days_cnt_next_daily"),
      col("payment_amount").alias("payment_amount_daily")
    )

  val balanceArray = base_tmp
    .join(balance, base_tmp("last_ctn")===balance("ctn")&&
      base_tmp("last_ban")===balance("ban")&&
      base_tmp("data") === balance("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(
      max(col("balance_daily")).as("balance_daily"),
      max(col("bal_neg_flg_daily")).as("bal_neg_flg_daily"),
      max(col("bal_low_flg_daily")).as("bal_low_flg_daily"),
      max(col("bal_high_flg_daily")).as("bal_high_flg_daily"),
      max(col("bal_is_enough_daily")).as("bal_is_enough_daily"),
      max(col("bal_delta_daily")).as("bal_delta_daily"),
      max(col("days_cnt_last_daily")).as("days_cnt_last_daily"),
      max(col("days_cnt_next_daily")).as("days_cnt_next_daily"),
      max(col("payment_amount_daily")).as("payment_amount_daily")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"${i}_balance_daily")): _*).as("balance_daily"),
      array((0 to 6).map(i => col(s"${i}_bal_neg_flg_daily")): _*).as("bal_neg_flg_daily"),
      array((0 to 6).map(i => col(s"${i}_bal_low_flg_daily")): _*).as("bal_low_flg_daily"),
      array((0 to 6).map(i => col(s"${i}_bal_high_flg_daily")): _*).as("bal_high_flg_daily"),
      array((0 to 6).map(i => col(s"${i}_bal_is_enough_daily")): _*).as("bal_is_enough_daily"),
      array((0 to 6).map(i => col(s"${i}_bal_delta_daily")): _*).as("bal_delta_daily"),
      array((0 to 6).map(i => col(s"${i}_days_cnt_last_daily")): _*).as("days_cnt_last_daily"),
      array((0 to 6).map(i => col(s"${i}_days_cnt_next_daily")): _*).as("days_cnt_next_daily"),
      array((0 to 6).map(i => col(s"${i}_payment_amount_daily")): _*).as("payment_amount_daily")
    )

  val balanceFeatures = base_tmp
    .join(balance, base_tmp("last_ctn")===balance("ctn")&&
      base_tmp("last_ban")===balance("ban")&&
      base_tmp("data") === balance("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("balance_daily")).alias("balance_daily_max"),
      max(col("bal_neg_flg_daily")).alias("bal_neg_flg_daily_max"),
      max(col("bal_low_flg_daily")).alias("bal_low_flg_daily_max"),
      max(col("bal_high_flg_daily")).alias("bal_high_flg_daily_max"),
      max(col("bal_is_enough_daily")).alias("bal_is_enough_daily_max"),
      sum(col("bal_neg_flg_daily")).alias("bal_neg_flg_daily_sum"),
      sum(col("bal_low_flg_daily")).alias("bal_low_flg_daily_sum"),
      sum(col("bal_high_flg_daily")).alias("bal_high_flg_daily_sum"),
      sum(col("bal_is_enough_daily")).alias("bal_is_enough_daily_sum"),
      avg(col("balance_daily")).alias("balance_daily_avg"),
      avg(col("bal_neg_flg_daily")).alias("bal_neg_flg_daily_avg"),
      avg(col("bal_low_flg_daily")).alias("bal_low_flg_daily_avg"),
      avg(col("bal_high_flg_daily")).alias("bal_high_flg_daily_avg"),
      avg(col("bal_is_enough_daily")).alias("bal_is_enough_daily_avg")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("balance_daily_max"),
      col("bal_neg_flg_daily_max"),
      col("bal_low_flg_daily_max"),
      col("bal_high_flg_daily_max"),
      col("bal_is_enough_daily_max"),
      col("bal_neg_flg_daily_sum"),
      col("bal_low_flg_daily_sum"),
      col("bal_high_flg_daily_sum"),
      col("bal_is_enough_daily_sum"),
      col("balance_daily_avg"),
      col("bal_neg_flg_daily_avg"),
      col("bal_low_flg_daily_avg"),
      col("bal_high_flg_daily_avg"),
      col("bal_is_enough_daily_avg")
    )

  val core_clients_sample_balance = balanceArray
    .join(balanceFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_balance
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_balance")

}
