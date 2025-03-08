package ru.beeline.cvm.datamarts.transform.adsDataset2_0

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, date_format, explode, lag, lit, mean, min, round, sum, to_date, trunc, udf}
import org.apache.spark.sql.types.DoubleType

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class AdsDataset2_0 extends Load {

  val DmMobileSubscrbiberUsageDaily = getDmMobileSubscrbiberUsageDaily
  val AggCtnParicePlansHistPubD = getAggCtnParicePlansHistPubD
  val balances_sdf_m = getBalancesSdfM


  val generateDatesUDF = udf((startDate: String, endDate: String, interval: Int) => {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

    // Parse the start and end dates
    val start = LocalDate.parse(startDate, formatter)
    val end = LocalDate.parse(endDate, formatter)

    // Generate dates
    var current = start
    var dates = Array[String]()
    while (!current.isAfter(end)) {
      dates = dates :+ current.format(formatter)
      current = current.plusDays(interval)
    }

    dates
  });

  val sdf_dm_mobile_subscriber_voice = DmMobileSubscrbiberUsageDaily
    .filter(col("activity_cd") === "VOICE")
    .filter(col("CALL_DIRECTION_CD") === 2)
    .groupBy("subscriber_num", "ban_num", "TRANSACTION_DT")
    .agg(
      sum(col("activity_amt")).as("activity_amt_sum"),
      sum(col("usage_amt")).as("usage_amt_sum")
    );

  val sdf_dm_mobile_subscriber_voice_grouped = sdf_dm_mobile_subscriber_voice
    .groupBy("subscriber_num", "ban_num")
    .agg(min(col("TRANSACTION_DT")).as("first_transaction_dt"))
    .withColumn("last_transaction_dt", to_date(lit(last_partition)))
    .withColumnRenamed("subscriber_num", "sub_key")
    .withColumnRenamed("ban_num", "ban_key")
    .withColumnRenamed("first_transaction_dt", "first_transaction_date")
    .withColumnRenamed("last_transaction_dt", "last_transaction_date");

  val sdf_voice_consec_dates = sdf_dm_mobile_subscriber_voice_grouped
    .withColumn("dates_array", generateDatesUDF(col("first_transaction_date"), col("last_transaction_date"), lit(1)))
    .withColumn("transaction_date", explode(col("dates_array")));

  val sdf_voice_transactions_final = sdf_voice_consec_dates
    .join(sdf_dm_mobile_subscriber_voice,
        sdf_voice_consec_dates("sub_key") === sdf_dm_mobile_subscriber_voice("subscriber_num") &&
        sdf_voice_consec_dates("ban_key") === sdf_dm_mobile_subscriber_voice("ban_num") &&
        sdf_voice_consec_dates("transaction_date") === sdf_dm_mobile_subscriber_voice("TRANSACTION_DT")
      , "left")
    .withColumn("transaction_date_month", trunc(col("transaction_date"), "Month"))
    .na.fill(0, Seq("activity_amt_sum", "usage_amt_sum"))
    .select(
      col("sub_key"),
      col("ban_key"),
      col("transaction_date"),
      col("transaction_date_month"),
      col("activity_amt_sum"),
      col("usage_amt_sum")
    );

  val sdf_voice_transactions_final_grouped = sdf_voice_transactions_final
    .groupBy("sub_key", "ban_key", "transaction_date_month")
    .agg(
      sum(col("activity_amt_sum")).as("activity_amt_sum_month"),
      sum(col("usage_amt_sum")).as("usage_amt_sum_month"));

  val sdf_dm_mobile_subscriber_gprs = DmMobileSubscrbiberUsageDaily
    .filter(col("activity_cd") === "GPRS")
    .groupBy(col("subscriber_num"), col("ban_num"), col("TRANSACTION_DT"))
    .agg(
      sum(col("activity_amt")).as("activity_amt_sum"),
      sum(col("usage_amt")).as("usage_amt_sum"));

  val sdf_dm_mobile_subscriber_gprs_grouped = sdf_dm_mobile_subscriber_gprs
    .groupBy("subscriber_num", "ban_num")
    .agg(min(col("TRANSACTION_DT")).as("first_transaction_dt"))
    .withColumn("last_transaction_dt", to_date(lit(last_partition)))
    .withColumnRenamed("subscriber_num", "sub_key")
    .withColumnRenamed("ban_num", "ban_key")
    .withColumnRenamed("first_transaction_dt", "first_transaction_date")
    .withColumnRenamed("last_transaction_dt", "last_transaction_date");

  val sdf_gprs_conces_dates = sdf_dm_mobile_subscriber_gprs_grouped
    .withColumn("dates_array", generateDatesUDF(col("first_transaction_date"), col("last_transaction_date"), lit(1)))
    .withColumn("transaction_date", explode(col("dates_array")));

  val sdf_gprs_transactions_final = sdf_gprs_conces_dates
    .join(sdf_dm_mobile_subscriber_gprs,
        sdf_gprs_conces_dates("sub_key") === sdf_dm_mobile_subscriber_gprs("subscriber_num") &&
        sdf_gprs_conces_dates("ban_key") === sdf_dm_mobile_subscriber_gprs("ban_num") &&
        sdf_gprs_conces_dates("transaction_date") === sdf_dm_mobile_subscriber_gprs("TRANSACTION_DT"),
      "left")
    .withColumn("transaction_date_month", trunc(col("transaction_date"), "Month"))
    .na.fill(0, Seq("activity_amt_sum", "usage_amt_sum"))
    .select(
      col("sub_key"),
      col("ban_key"),
      col("transaction_date"),
      col("transaction_date_month"),
      col("activity_amt_sum"),
      col("usage_amt_sum"));

  val sdf_gprs_transactions_final_grouped = sdf_gprs_transactions_final
    .groupBy("sub_key", "ban_key", "transaction_date_month")
    .agg(
      sum(col("activity_amt_sum")).as("activity_amt_sum_month"),
      sum(col("usage_amt_sum")).as("usage_amt_sum_month")
    );

  val lag_window_3_1 =
    Window
      .partitionBy(col("sub_key"), col("ban_key"))
      .orderBy(col("transaction_date_month"))
      .rowsBetween(-3, -1);

  val lag_window_1 =
    Window
      .partitionBy(col("sub_key"), col("ban_key"))
      .orderBy(col("transaction_date_month"));

  val sdf_dm_mobile_group_voice_lag = sdf_voice_transactions_final_grouped
    .withColumn("activity_amt_sum_voice", col("activity_amt_sum_month").cast(DoubleType))
    .withColumn("usage_amt_sum_voice", round(col("usage_amt_sum_month").cast(DoubleType) / 60.0, 3))
    .withColumn("_1_activity_amt_sum_voice", lag("activity_amt_sum_voice", 1).over(lag_window_1))
    .withColumn("_1_usage_amt_sum_voice", lag("usage_amt_sum_voice", 1).over(lag_window_1))
    .withColumn("_3_activity_amt_avg_voice", round(mean(col("activity_amt_sum_voice")).over(lag_window_3_1), 3))
    .withColumn("_3_usage_amt_avg_voice", round(mean(col("usage_amt_sum_voice")).over(lag_window_3_1), 3))
    .withColumnRenamed("sub_key", "sub_key_voice")
    .withColumnRenamed("ban_key", "ban_key_voice")
    .withColumn("transaction_date_month", date_format(col("transaction_date_month"), "yyyy-MM-dd"))
    .withColumnRenamed("transaction_date_month", "transaction_date_month_voice")
    .drop(col("activity_amt_sum_month"))
    .drop(col("usage_amt_sum_month"));

  val sdf_dm_mobile_group_gprs_lag = sdf_gprs_transactions_final_grouped
    .withColumn("activity_amt_sum_gprs", col("activity_amt_sum_month").cast(DoubleType))
    .withColumn("usage_amt_sum_gprs", round((col("usage_amt_sum_month").cast(DoubleType)) / 1024.0 / 1024.0 / 1024.0, 3))
    .withColumn("_1_activity_amt_sum_gprs", lag("activity_amt_sum_gprs", 1).over(lag_window_1))
    .withColumn("_1_usage_amt_sum_gprs", lag("usage_amt_sum_gprs", 1).over(lag_window_1))
    .withColumn("_3_activity_amt_avg_gprs", round(mean(col("activity_amt_sum_gprs")).over(lag_window_3_1), 3))
    .withColumn("_3_usage_amt_avg_gprs", round(mean(col("usage_amt_sum_gprs")).over(lag_window_3_1), 3))
    .withColumnRenamed("sub_key", "sub_key_gprs")
    .withColumnRenamed("ban_key", "ban_key_gprs")
    .drop(col("activity_amt_sum_month"))
    .drop(col("usage_amt_sum_month"))
    .withColumn("transaction_date_month", date_format(col("transaction_date_month"), "yyyy-MM-dd"))
    .withColumnRenamed("transaction_date_month", "transaction_date_month_gprs");

  val current_sdf_voice = AggCtnParicePlansHistPubD
    .join(sdf_dm_mobile_group_voice_lag,
        AggCtnParicePlansHistPubD("SUBS_KEY") === sdf_dm_mobile_group_voice_lag("sub_key_voice") &&
        AggCtnParicePlansHistPubD("BAN_KEY") === sdf_dm_mobile_group_voice_lag("ban_key_voice") &&
        AggCtnParicePlansHistPubD("time_key_monthly") === sdf_dm_mobile_group_voice_lag("transaction_date_month_voice")
      , "left");

  val current_sdf_voice_gprs = current_sdf_voice
    .join(sdf_dm_mobile_group_gprs_lag,
        current_sdf_voice("SUBS_KEY") === sdf_dm_mobile_group_gprs_lag("sub_key_gprs") &&
        current_sdf_voice("BAN_KEY") === sdf_dm_mobile_group_gprs_lag("ban_key_gprs") &&
        current_sdf_voice("time_key_monthly") === sdf_dm_mobile_group_gprs_lag("transaction_date_month_gprs")
      , "left");

  val ads_dataset = current_sdf_voice_gprs
    .join(balances_sdf_m,
        current_sdf_voice_gprs("SUBS_KEY") === balances_sdf_m("subs_key_balances") &&
        current_sdf_voice_gprs("BAN_KEY") === balances_sdf_m("ban_key_balances")
      , "left");

}
