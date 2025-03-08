package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions.{array, avg, col, max}

object CoreClientsSMSData extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val sms = spark.read.table(tableAggCtnSmsCatPubD)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("sms_bank_cnt").alias("sms_bank_cnt_daily"),
      col("sms_shop_cnt").alias("sms_shop_cnt_daily"),
      col("sms_otp_cnt").alias("sms_otp_cnt_daily")
    )

  val smsArray = dayInflow
    .join(sms, dayInflow("subs_key")===sms("first_ctn")&&
      dayInflow("ban_key")===sms("first_ban")&&
      dayInflow("data") === sms("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(
      max(col("sms_bank_cnt_daily")).as("sms_bank_cnt_daily"),
      max(col("sms_shop_cnt_daily")).as("sms_shop_cnt_daily"),
      max(col("sms_otp_cnt_daily")).as("sms_otp_cnt_daily")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"${i}_sms_bank_cnt_daily")): _*).as("sms_bank_cnt_daily"),
      array((0 to 6).map(i => col(s"${i}_sms_shop_cnt_daily")): _*).as("sms_shop_cnt_daily"),
      array((0 to 6).map(i => col(s"${i}_sms_otp_cnt_daily")): _*).as("sms_otp_cnt_daily")
    )

  val smsFeatures = dayInflow
    .join(sms, dayInflow("subs_key")===sms("first_ctn")&&
      dayInflow("ban_key")===sms("first_ban")&&
      dayInflow("data") === sms("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("sms_bank_cnt_daily")).alias("sms_bank_cnt_daily_max"),
      max(col("sms_shop_cnt_daily")).alias("sms_shop_cnt_daily_max"),
      max(col("sms_otp_cnt_daily")).alias("sms_otp_cnt_daily_max"),
      avg(col("sms_bank_cnt_daily")).alias("sms_bank_cnt_daily_avg"),
      avg(col("sms_shop_cnt_daily")).alias("sms_shop_cnt_daily_avg"),
      avg(col("sms_otp_cnt_daily")).alias("sms_otp_cnt_daily_avg")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("sms_bank_cnt_daily_max"),
      col("sms_shop_cnt_daily_max"),
      col("sms_otp_cnt_daily_max"),
      col("sms_bank_cnt_daily_avg"),
      col("sms_shop_cnt_daily_avg"),
      col("sms_otp_cnt_daily_avg")
    )

  val core_clients_sample_sms = smsArray
    .join(smsFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_sms
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_sms")

}
