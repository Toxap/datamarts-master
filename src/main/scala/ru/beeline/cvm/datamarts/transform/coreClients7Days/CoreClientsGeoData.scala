package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

object CoreClientsGeoData extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val customers_geo = spark.read.table(tableCustomersPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "time_key_cust")
    .select(
      col("first_ban"),
      col("first_ctn"),
      col("last_ctn"),
      col("last_ban"),
      col("time_key_cust"),
      col("market_key")
    )

  val pos_code_sdf = spark.read.table(tableAggTimeSpendPositionCodesPubD)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .select(
      col("ctn"),
      col("ban"),
      col("market"),
      col("position_code"),
      col("time_key")
    )

  val pos_code_sdf_table = pos_code_sdf
    .join(customers_geo, pos_code_sdf("ctn")===customers_geo("last_ctn") &&
      pos_code_sdf("ban")===customers_geo("last_ban") &&
      pos_code_sdf("time_key")===customers_geo("time_key_cust") &&
      pos_code_sdf("market")===customers_geo("market_key"), "left")
    .withColumn("first_ctn", coalesce(col("first_ctn"), col("ctn")))
    .withColumn("first_ban", coalesce(col("first_ban"), col("ban")))
    .withColumn("first_ctn_is_null", when(col("first_ctn").isNull, 0).otherwise(0))

  val pos_code_sdf_uniq_cnt_every_time_key = pos_code_sdf_table
    .groupBy("first_ctn", "first_ban", "market", "time_key")
    .agg(countDistinct(col("position_code")).alias("uniq_amt_pos_code"))

  val sft_geo = dayInflow
    .join(pos_code_sdf_uniq_cnt_every_time_key, dayInflow("subs_key") === pos_code_sdf_uniq_cnt_every_time_key("first_ctn") &&
      dayInflow("ban_key") === pos_code_sdf_uniq_cnt_every_time_key("first_ban")&&
      dayInflow("data") === pos_code_sdf_uniq_cnt_every_time_key("time_key"), "left")
    .withColumn("uniq_amt_pos_code_nonull", when(col("uniq_amt_pos_code").isNull, 0).otherwise(col("uniq_amt_pos_code")))

  val geoArray = sft_geo
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(max(col("uniq_amt_pos_code")).as("uniq_amt_pos_code_daily"))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"$i")): _*).as("uniq_amt_pos_code_daily")
    )

  val geoFeatures = sft_geo
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(max("uniq_amt_pos_code_nonull").alias("uniq_amt_pos_code_daily_max"))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("uniq_amt_pos_code_daily_max")
    )

  val core_clients_sample_geo = geoArray
    .join(geoFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_geo
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_geo")

}
