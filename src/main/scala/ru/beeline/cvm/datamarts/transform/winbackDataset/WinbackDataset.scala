package ru.beeline.cvm.datamarts.transform.winbackDataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, TimestampType}


object WinbackDataset extends Load{

    val aggCtnBalancePubM = getCtnBalance()
    val aggCtnSmsCatPubM = getSms()
    val aggCtnUsagePubD = getUsageCtn()
    val fctActClustSubsMPub = getFctActClust()

    val res = aggCtnBalancePubM
      .join(aggCtnSmsCatPubM, Seq("subs_key", "ban_key"))
      .join(aggCtnUsagePubD, Seq("subs_key", "ban_key"))
      .join(fctActClustSubsMPub, Seq("subs_key", "ban_key"))
      .select(
        col("subs_key"),
        col("ban_key"),
        aggCtnBalancePubM.col("first_time_key").cast(TimestampType),
        col("market_key"),
        col("normalized_bal_neg_cnt_m_mean_lag_2_4").cast(FloatType),
        col("normalized_bal_neg_cnt_m_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_sms_bank_cnt_m_mean_lag_2_4").cast(FloatType),
        col("normalized_sms_bank_cnt_m_stddev_samp_lag_2_4").cast(FloatType),
        col("internet_active_flg_mean_lag_1_14").cast(FloatType),
        col("voice_out_active_flg_mean_lag_1_14").cast(FloatType),
        col("voice_input_active_flg_mean_lag_1_14").cast(FloatType),
        col("normalized_direct_rev_mean_lag_2_4").cast(FloatType),
        col("normalized_direct_rev_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_total_rev_mean_lag_2_4").cast(FloatType),
        col("normalized_total_rev_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_mou_out_bln_mean_lag_2_4").cast(FloatType),
        col("normalized_mou_out_bln_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_cnt_voice_day_mean_lag_2_4").cast(FloatType),
        col("normalized_cnt_voice_day_stddev_samp_lag_2_4").cast(FloatType),
        col("bs_data_rate_mean_lag_2_4").cast(FloatType),
        col("bs_data_rate_stddev_samp_lag_2_4").cast(FloatType),
        col("bs_voice_rate_mean_lag_2_4").cast(FloatType),
        col("bs_voice_rate_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_total_recharge_amt_rur_mean_lag_2_4").cast(FloatType),
        col("normalized_total_recharge_amt_rur_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_total_contact_mean_lag_2_4").cast(FloatType),
        col("normalized_total_contact_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_cnt_data_day_mean_lag_2_4").cast(FloatType),
        col("normalized_cnt_data_day_stddev_samp_lag_2_4").cast(FloatType),
        col("normalized_cnt_recharge_mean_lag_2_4").cast(FloatType),
        col("normalized_cnt_recharge_stddev_samp_lag_2_4").cast(FloatType),
        col("cluster_num_max_lag_2").cast(IntegerType),
        col("is_smartphone_max_lag_2").cast(IntegerType),
        col("permanent_aab_max_lag_2").cast(IntegerType),
        lit(SCORE_DATE).as("time_key")
      )

    res.repartition(5)
      .write
      .format("orc")
      .mode("overwrite")
      .insertInto(TableWinbackDataset)
}
