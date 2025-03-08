package ru.beeline.cvm.datamarts.transform.adsDataset2_0

import org.apache.spark.sql.functions.{col, lit}

object Main extends AdsDataset2_0 {

  val res = ads_dataset
    .select(
        col("SUBS_KEY"),
        col("BAN_KEY").cast("long"),
        col("MARKET_KEY"),
        col("soc_pp_current"),
        col("constructor_id_current"),
        col("COUNT_VOICE_MIN_CURRENT"),
        col("COUNT_DATA_GB_CURRENT"),
        col("MONTHLY_FEE_CURRENT"),
        col("DAILY_FEE_CURRENT"),
        col("_1_activity_amt_sum_voice"),
        col("_1_usage_amt_sum_voice"),
        col("_3_activity_amt_avg_voice"),
        col("_3_usage_amt_avg_voice"),
        col("_1_activity_amt_sum_gprs"),
        col("_1_usage_amt_sum_gprs"),
        col("_3_activity_amt_avg_gprs"),
        col("_3_usage_amt_avg_gprs"),
        col("BAL_MAX_3M"),
        lit(LOAD_DATE_YYYY_MM_DD).as("report_dt")
    );

    res
      .repartition(5)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableAdsDataset20)

}
