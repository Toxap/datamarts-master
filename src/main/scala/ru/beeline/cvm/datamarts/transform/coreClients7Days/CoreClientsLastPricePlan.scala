package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions.{array, col, max}

object CoreClientsLastPricePlan extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")

  val lastPP = spark.read.table(tableLastPricePlanPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("price_plan_key").alias("price_plan_key_daily"),
      col("last_pp_monthly_fee").alias("last_pp_monthly_fee_daily"),
      col("last_pp_daily_fee").alias("last_pp_daily_fee_daily"),
      col("last_pp_data_volume").alias("last_pp_data_volume_daily"),
      col("last_pp_voice_volume").alias("last_pp_voice_volume_daily"),
      col("last_pp_archive_ind").alias("last_pp_archive_ind_daily"),
      col("last_pp_flat_ind").alias("last_pp_flat_ind_daily"),
      col("last_pp_unlimited_data").alias("last_pp_unlimited_data_daily"),
      col("days_from_last_pp_started").alias("days_from_last_pp_started_daily")
    )

  val core_clients_sample_last_price_plan = dayInflow
    .join(lastPP, dayInflow("subs_key")===lastPP("first_ctn")&&
      dayInflow("ban_key")===lastPP("first_ban")&&
      dayInflow("data") === lastPP("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(
      max(col("price_plan_key_daily")).as("price_plan_key_daily"),
      max(col("last_pp_monthly_fee_daily")).as("last_pp_monthly_fee_daily"),
      max(col("last_pp_daily_fee_daily")).as("last_pp_daily_fee_daily"),
      max(col("last_pp_data_volume_daily")).as("last_pp_data_volume_daily"),
      max(col("last_pp_voice_volume_daily")).as("last_pp_voice_volume_daily"),
      max(col("last_pp_archive_ind_daily")).as("last_pp_archive_ind_daily"),
      max(col("last_pp_flat_ind_daily")).as("last_pp_flat_ind_daily"),
      max(col("last_pp_unlimited_data_daily")).as("last_pp_unlimited_data_daily"),
      max(col("days_from_last_pp_started_daily")).as("days_from_last_pp_started_daily")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"${i}_price_plan_key_daily")): _*).as("price_plan_key_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_monthly_fee_daily")): _*).as("last_pp_monthly_fee_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_daily_fee_daily")): _*).as("last_pp_daily_fee_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_data_volume_daily")): _*).as("last_pp_data_volume_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_voice_volume_daily")): _*).as("last_pp_voice_volume_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_archive_ind_daily")): _*).as("last_pp_archive_ind_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_flat_ind_daily")): _*).as("last_pp_flat_ind_daily"),
      array((0 to 6).map(i => col(s"${i}_last_pp_unlimited_data_daily")): _*).as("last_pp_unlimited_data_daily"),
      array((0 to 6).map(i => col(s"${i}_days_from_last_pp_started_daily")): _*).as("days_from_last_pp_started_daily")
    )

  core_clients_sample_last_price_plan
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_last_price_plan")

}
