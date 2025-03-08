package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

object CoreClientsTrafficData extends CustomParams {
  val dayInflow = spark.read.table("nba_engine.am_core_clients_sample_base_7d")
  val data = spark.read.table(tableDataAllPub)
    .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_14)
    .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_8)
    .withColumnRenamed("time_key", "feat_dt")
    .select(
      col("feat_dt"),
      col("first_ctn"),
      col("first_ban"),
      col("data_all_mb").alias("data_all_mb_daily")
    )

  val dataArray = dayInflow
    .join(data, dayInflow("subs_key")===data("first_ctn")&&
      dayInflow("ban_key")===data("first_ban")&&
      dayInflow("data") === data("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .pivot("day", (0 to 6).toList.map(_.toString))
    .agg(max(col("data_all_mb_daily")).as("data_all_mb_daily"))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      array((0 to 6).map(i => col(s"$i")): _*).as("data_all_mb_daily")
    )

  val dataFeatures = dayInflow
    .join(data, dayInflow("subs_key")===data("first_ctn")&&
      dayInflow("ban_key")===data("first_ban")&&
      dayInflow("data") === data("feat_dt"), "left")
    .groupBy("subs_key", "ban_key", "market_key", "sale_dt", "load_date", "date_key", "business_type")
    .agg(
      max(col("data_all_mb_daily")).alias("data_all_mb_daily_max"),
      avg(col("data_all_mb_daily")).alias("data_all_mb_daily_avg"),
      stddev(col("data_all_mb_daily")).alias("data_all_mb_daily_std")
    )
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("load_date"),
      col("date_key"),
      col("business_type"),
      col("data_all_mb_daily_max"),
      col("data_all_mb_daily_avg"),
      col("data_all_mb_daily_std")
    )

  val core_clients_sample_traffic = dataArray
    .join(dataFeatures, Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"), "inner")

  core_clients_sample_traffic
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_traffic")

}
