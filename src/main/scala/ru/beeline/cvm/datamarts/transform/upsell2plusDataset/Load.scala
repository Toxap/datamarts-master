package ru.beeline.cvm.datamarts.transform.upsell2plusDataset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Load extends CustomParams {
  def getSubscribers: DataFrame = {
    val listCurrStatus: List[String] = List("A", "S")

    spark.read.table(tableDimSubscriberPub)
      .filter(col("curr_dw_status").isin(listCurrStatus: _*) &&
        col("account_type_key") === 13)
      .select(
        col("subs_key").as("last_subs_key"),
        col("ban_key").as("last_ban_key"),
        col("market_key_src").as("market_key"))
  }

  def getSubscriberHistory: DataFrame = spark
    .read
    .table(tableFctActClustSubsM)
    .filter(col("time_key").between(LOAD_DATE_PYYYYMM_4M, LOAD_DATE_PYYYYMM))

  def getBans: DataFrame = spark.read.table(tableDimBan)

  def getSubscriberArpu: DataFrame = spark.read.table(tableAggArpuMou)
    .filter(col("time_key").between(LOAD_DATE_PYYYYMM_4M, LOAD_DATE_PYYYYMM_2M))
    .na.fill(0, Seq("total_loc_charge_amt_r", "total_ld_charge_amt_r", "total_int_charge_amt_r",
    "total_innet_roam_charge_amt_r", "total_nat_roam_charge_amt_r", "total_inter_roam_charge_amt_r",
    "total_out_sms_charge_amt_r", "total_add_srv_charge_amt_r", "total_rc_charges_amt_r",
    "total_adm_charges_amt_r", "total_discount_amt_r", "total_gprs_charge_amt_r", "total_out_dur_actual",
    "total_in_dur_actual", "out_sms_count", "gprs_rounded_data"))

  def aggregateSubscriberArpu(subscriberArpu: DataFrame): DataFrame = subscriberArpu
      .groupBy(col("ban_key").as("last_ban_key"),
        col("subs_key").as("last_subs_key"),
        col("market_key_src").as("market_key"))
      .agg(round(sum(col("total_loc_charge_amt_r") +
        col("total_ld_charge_amt_r") +
        col("total_int_charge_amt_r") +
        col("total_innet_roam_charge_amt_r") +
        col("total_nat_roam_charge_amt_r") +
        col("total_inter_roam_charge_amt_r") +
        col("total_out_sms_charge_amt_r") +
        col("total_add_srv_charge_amt_r") +
        col("total_rc_charges_amt_r") +
        col("total_adm_charges_amt_r") +
        col("total_discount_amt_r") +
        col("total_gprs_charge_amt_r")), 2).cast("double").as("total_arpu"),
        round(sum(col("total_out_dur_actual")) / 60, 2).cast("double").as("out_voice_traffic"),
        round(sum(col("total_in_dur_actual")) / 60, 2).cast("double").as("in_voice_traffic"),
        sum(col("out_sms_count")).cast("double").as("out_sms_count"),
        (sum(col("gprs_rounded_data")) / 1024 / 1024 / 1024).cast("double").as("data_traffic"))

  def getTariffs: DataFrame = spark.read.table(tableDimSocParameter)
      .na.fill(0, Array("monthly_fee", "daily_fee", "cnt_voice_local", "count_gprs", "count_sms"))
      .select(
        col("soc_code"),
        col("market_key").as("market_key_dim_soc"),
        when(col("monthly_fee") > 0, col("monthly_fee"))
          .otherwise(when(col("monthly_fee") === 0, col("daily_fee") * 30)).as("monthly_fee_dim_soc"),
        col("cnt_voice_local").as("minutes"),
        col("count_gprs").as("gb"),
        col("count_sms").as("sms"))
}
