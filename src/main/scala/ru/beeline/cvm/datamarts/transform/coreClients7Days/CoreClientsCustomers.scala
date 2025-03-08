package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions.{array, col, max}

object CoreClientsCustomers extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")

  val customers = spark.read.table(tableCustomersPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("curr_subs_status_key").alias("curr_subs_status_key_daily"),
      col("market_key").alias("market_key_daily"),
      col("super_region_name_eng").alias("super_region_name_eng_daily"),
      col("account_type_key").alias("account_type_key_daily"),
      col("business_type").alias("business_type_daily"),
      col("ban_type_desc").alias("ban_type_desc_daily"),
      col("pp_archetype").alias("pp_archetype_daily"),
      col("recipient_ind").alias("recipient_ind_daily"),
      col("donor_ind").alias("donor_ind_daily"),
      col("lifetime").alias("lifetime_daily"),
      col("last_usage_trafficout").alias("last_usage_trafficout_daily"),
      col("last_significant_usage").alias("last_significant_usage_daily")
    )

  val core_sample_customers = dayInflow
    .join(customers, dayInflow("subs_key") === customers("first_ctn") &&
      dayInflow("ban_key") === customers("first_ban") &&
      dayInflow("data") === customers("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(
      max(col("curr_subs_status_key_daily")).as("curr_subs_status_key_daily"),
      max(col("market_key_daily")).as("market_key_daily"),
      max(col("super_region_name_eng_daily")).as("super_region_name_eng_daily"),
      max(col("account_type_key_daily")).as("account_type_key_daily"),
      max(col("business_type_daily")).as("business_type_daily"),
      max(col("ban_type_desc_daily")).as("ban_type_desc_daily"),
      max(col("pp_archetype_daily")).as("pp_archetype_daily"),
      max(col("recipient_ind_daily")).as("recipient_ind_daily"),
      max(col("donor_ind_daily")).as("donor_ind_daily"),
      max(col("lifetime_daily")).as("lifetime_daily"),
      max(col("last_usage_trafficout_daily")).as("last_usage_trafficout_daily"),
      max(col("last_significant_usage_daily")).as("last_significant_usage_daily")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"${i}_curr_subs_status_key_daily")): _*).as("curr_subs_status_key_daily"),
      array((0 to 6).map(i => col(s"${i}_market_key_daily")): _*).as("market_key_daily"),
      array((0 to 6).map(i => col(s"${i}_super_region_name_eng_daily")): _*).as("super_region_name_eng_daily"),
      array((0 to 6).map(i => col(s"${i}_account_type_key_daily")): _*).as("account_type_key_daily"),
      array((0 to 6).map(i => col(s"${i}_business_type_daily")): _*).as("business_type_daily"),
      array((0 to 6).map(i => col(s"${i}_ban_type_desc_daily")): _*).as("ban_type_desc_daily"),
      array((0 to 6).map(i => col(s"${i}_pp_archetype_daily")): _*).as("pp_archetype_daily"),
      array((0 to 6).map(i => col(s"${i}_recipient_ind_daily")): _*).as("recipient_ind_daily"),
      array((0 to 6).map(i => col(s"${i}_donor_ind_daily")): _*).as("donor_ind_daily"),
      array((0 to 6).map(i => col(s"${i}_lifetime_daily")): _*).as("lifetime_daily"),
      array((0 to 6).map(i => col(s"${i}_last_usage_trafficout_daily")): _*).as("last_usage_trafficout_daily"),
      array((0 to 6).map(i => col(s"${i}_last_significant_usage_daily")): _*).as("last_significant_usage_daily")
    )

  core_sample_customers
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_customers")

}
