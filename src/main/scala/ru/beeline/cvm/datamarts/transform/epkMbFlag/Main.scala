package ru.beeline.cvm.datamarts.transform.epkMbFlag

import org.apache.spark.sql.functions.{col, lit, to_timestamp}

object Main extends CustomParams {
  val limitPercent = args(0).toDouble

  val dmMobileSubscriber = spark.table("beemetrics.dm_mobile_subscriber")
    .filter(col("calendar_dt") === SCORE_DATE_4_days)
    .filter(col("account_type_cd")===13)
    .filter(col("subscriber_status_cd").isin("A", "S"))
    .select(
        col("subscriber_num").as("subs_key"),
        col("ban_num").as("ban_key"))
    .distinct()
    .count();

  val countSubsWithPercent = (dmMobileSubscriber * limitPercent).toInt

  val resultDF = spark.table("beemetrics.dm_mobile_subscriber")
    .filter(col("calendar_dt") === SCORE_DATE_4_days)
    .filter(col("account_type_cd")===13)
    .filter(col("subscriber_status_cd").isin("A", "S"))
    .select(
        col("subscriber_num").as("ctn"),
        col("ban_num").as("ban_key"),
      to_timestamp(lit(SCORE_DATE)).as("date_actual_from"),
      to_timestamp(lit("3000-01-01")).as("date_actual_to")
    )
    .limit(countSubsWithPercent)

  resultDF
    .repartition(1)
    .write
    .mode("overwrite")
    .format("parquet")
    .insertInto("nba_engine.epk_mb_flag")
}
