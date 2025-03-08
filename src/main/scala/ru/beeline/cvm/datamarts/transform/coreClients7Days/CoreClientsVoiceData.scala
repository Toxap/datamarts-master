package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

object CoreClientsVoiceData extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")

  val voice = spark.read.table(tableVoiceAllPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("voice_in_sec").alias("voice_in_sec_daily"),
      col("voice_out_sec").alias("voice_out_sec_daily"),
      col("voice_in_cnt").alias("voice_in_cnt_daily"),
      col("voice_out_cnt").alias("voice_out_cnt_daily")
    )

  val voiceArray = dayInflow
    .join(voice, dayInflow("subs_key") === voice("first_ctn") &&
      dayInflow("ban_key") === voice("first_ban")&&
      dayInflow("data") === voice("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(
      max(col("voice_in_sec_daily")).as("voice_in_sec_daily"),
      max(col("voice_out_sec_daily")).as("voice_out_sec_daily"),
      max(col("voice_in_cnt_daily")).as("voice_in_cnt_daily"),
      max(col("voice_out_cnt_daily")).as("voice_out_cnt_daily")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"${i}_voice_in_sec_daily")): _*).as("voice_in_sec_daily"),
      array((0 to 6).map(i => col(s"${i}_voice_out_sec_daily")): _*).as("voice_out_sec_daily"),
      array((0 to 6).map(i => col(s"${i}_voice_in_cnt_daily")): _*).as("voice_in_cnt_daily"),
      array((0 to 6).map(i => col(s"${i}_voice_out_cnt_daily")): _*).as("voice_out_cnt_daily")
    )

  val voiceFeatures = dayInflow
    .join(voice, dayInflow("subs_key") === voice("first_ctn") &&
      dayInflow("ban_key") === voice("first_ban")&&
      dayInflow("data") === voice("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("voice_in_sec_daily")).alias("voice_in_sec_daily_max"),
      max(col("voice_out_sec_daily")).alias("voice_out_sec_daily_max"),
      max(col("voice_in_cnt_daily")).alias("voice_in_cnt_daily_max"),
      max(col("voice_out_cnt_daily")).alias("voice_out_cnt_daily_max"),
      avg(col("voice_in_sec_daily")).alias("voice_in_sec_daily_avg"),
      avg(col("voice_out_sec_daily")).alias("voice_out_sec_daily_avg"),
      avg(col("voice_in_cnt_daily")).alias("voice_in_cnt_daily_avg"),
      avg(col("voice_out_cnt_daily")).alias("voice_out_cnt_daily_avg"),
      stddev(col("voice_in_sec_daily")).alias("voice_in_sec_daily_std"),
      stddev(col("voice_out_sec_daily")).alias("voice_out_sec_daily_std"),
      stddev(col("voice_in_cnt_daily")).alias("voice_in_cnt_daily_std"),
      stddev(col("voice_out_cnt_daily")).alias("voice_out_cnt_daily_std")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("voice_in_sec_daily_max"),
      col("voice_out_sec_daily_max"),
      col("voice_in_cnt_daily_max"),
      col("voice_out_cnt_daily_max"),
      col("voice_in_sec_daily_avg"),
      col("voice_out_sec_daily_avg"),
      col("voice_in_cnt_daily_avg"),
      col("voice_out_cnt_daily_avg"),
      col("voice_in_sec_daily_std"),
      col("voice_out_sec_daily_std"),
      col("voice_in_cnt_daily_std"),
      col("voice_out_cnt_daily_std")
    )

  val core_clients_sample_voice = voiceArray
    .join(voiceFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_voice
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_voice")

}
