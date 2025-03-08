package ru.beeline.cvm.datamarts.transform.adsDataset2_0

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, max, trunc, to_timestamp}
import org.apache.spark.sql.types.{IntegerType, StringType}

import java.time.format.DateTimeFormatter
import java.time.LocalDate

class Load extends CustomParams {

  val last_partition = spark.table(TableAggCtnPricePlansHistPubD)
    .select(max(col("time_key").cast("string")))
    .collect()(0)(0)
    .toString;

  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  val lastPartitionsDate: LocalDate = LocalDate.parse(last_partition, formatter);
  val startDate: String = lastPartitionsDate.minusMonths(6).format(formatter);

  def getAggCtnParicePlansHistPubD: DataFrame = {
    spark.table(TableAggCtnPricePlansHistPubD)
      .withColumn("time_key", date_format(col("time_key"), "yyyy-MM-dd"))
      .withColumn("time_key_monthly", date_format(trunc(col("time_key"), "Month"), "yyyy-MM-dd"))
      .filter(col("time_key") === last_partition)
      .withColumn("ban", col("ban").cast(IntegerType).cast(StringType))
      .select(
        col("ctn").as("SUBS_KEY"),
        col("ban").as("BAN_KEY"),
        col("market").as("MARKET_KEY"),
        col("soc_pp").as("soc_pp_current"),
        col("constructor_id").as("constructor_id_current"),
        col("voice_package_min").as("COUNT_VOICE_MIN_CURRENT"),
        col("data_package_gb").as("COUNT_DATA_GB_CURRENT"),
        col("monthly_fee").as("MONTHLY_FEE_CURRENT"),
        col("daily_fee").as("DAILY_FEE_CURRENT"),
        col("pp_effective_date"),
        col("time_key"),
        col("time_key_monthly")
      )
  }

  def getDmMobileSubscrbiberUsageDaily: DataFrame = {
    spark.table(TableDMMOBILESUBSCRIBERUSAGEDAILY)
      .filter(col("TRANSACTION_DT") >= startDate && col("TRANSACTION_DT") <= last_partition)
      .withColumn("ban_num", col("ban_num").cast(IntegerType).cast(StringType))
      .select(
        col("subscriber_num"),
        col("ban_num"),
        col("activity_cd"),
        col("TRANSACTION_DT"),
        col("activity_amt"),
        col("usage_amt")
      )
  }

  val last_partitions_balances =
    spark.table(TableAGGCTNBALANCEPERIODPUBM)
    .select(max(col("time_key").cast("string")))
    .collect()(0)(0)
    .toString;

  def getBalancesSdfM: DataFrame = {
    spark.read.table(TableAGGCTNBALANCEPERIODPUBM)
      .filter(col("time_key") === last_partitions_balances)
      .withColumn("ban", col("ban").cast(IntegerType).cast(StringType))
      .withColumn("time_key", to_timestamp(col("time_key"), "yyyy-MM-dd"))
      .select(col("ctn"),
        col("ban"),
        col("time_key"),
        col("BAL_MAX_3M"))
      .withColumnRenamed("ctn", "subs_key_balances")
      .withColumnRenamed("ban", "ban_key_balances")
      .withColumnRenamed("time_key", "time_key_balances")
  };

}
