package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object AllFeatures extends CustomParams {
  val core_clients_base = spark.read.table("nba_engine.am_core_clients_sample_base")
  val core_clients_customers = spark.read.table("nba_engine.am_core_clients_sample_customers")
  val core_clients_voice_data = spark.read.table("nba_engine.am_core_clients_sample_voice")
  val core_clients_traffic = spark.read.table("nba_engine.am_core_clients_sample_traffic")
  val core_clients_charges = spark.read.table("nba_engine.am_core_clients_sample_charges")
  val core_clients_recharges = spark.read.table("nba_engine.am_core_clients_sample_recharges")
  val core_clients_balance = spark.read.table("nba_engine.am_core_clients_sample_balance")
  val core_clients_sms = spark.read.table("nba_engine.am_core_clients_sample_sms")
  val core_clients_last_price_plan = spark.read.table("nba_engine.am_core_clients_sample_last_price_plan")
  val core_clients_geo = spark.read.table("nba_engine.am_core_clients_sample_geo")

  val first = udf((lst: Seq[String]) => {
    Option(lst.head)
  })

  val last = udf((lst: Seq[String]) => {
    Option(lst.last)
  })

  val first_pos = udf((lst: Seq[String]) => {
    val res = lst.map(x => if (x == null) "0.0" else x)
    Option(res.indexWhere(_ != "0.0")).filter(_ != -1).map(_ + 1)
  })

  val ratio_pos = udf((lst: Seq[String]) => {
    val new_lst = lst.map {
      case i if i != "0.0" => i;
      case _ => null
    }
    val nonNullLst = new_lst.filter(_ != null).map(_.toDouble)
    val cntNotNull = nonNullLst.size.toDouble
    Try(cntNotNull / new_lst.size) match {
      case Success(value) if !value.isNaN => Some(value);
      case Success(value) if value.isNaN => None;
      case Failure(exception) => null
    }
  })

  val ratio_pos_since_first = udf((lst: Seq[String]) => {
    val new_lst = lst.map { case i if i != "0.0" => i; case _ => null }
    val nonNullLst = new_lst.filter(_ != null).map(_.toDouble)
    val cntNotNull = nonNullLst.size.toDouble
    val indexNNValue = new_lst.find(_ != null).map(new_lst.indexOf).getOrElse(0)
    Try(cntNotNull / (new_lst.size - indexNNValue)) match {
      case Success(value) => Some(value);
      case Failure(exception) => null
    }
  })

  val cnt = udf((lst: Seq[String]) => {
    val new_lst = lst.map { case i if i != "0.0" => i; case _ => null }
    val nonNullLst = new_lst.filter(_ != null).map(_.toDouble)
    nonNullLst.size.toDouble
  })

  val avg_geo = udf((lst: Seq[String]) => {
    val sum = lst.map(x => if (x == null) "0.0" else x).filter(_ != null).map(_.toDouble).sum
    val cnt = lst.size.toDouble
    sum / cnt
  })

  val res = core_clients_base
    .join(core_clients_customers,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_voice_data,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_traffic,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_charges,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_recharges,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_last_price_plan,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_sms,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_balance,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .join(core_clients_geo,
      Seq("subs_key", "ban_key","market_key", "sale_dt", "load_date", "date_key", "business_type"),
      "inner")
    .drop(col("load_date"))
    .withColumn("report_dt", lit(LOAD_DATE_YYYY_MM_DD_plus1).cast("string"))
    .withColumn("uniq_amt_pos_code_daily_avg", avg_geo(col("uniq_amt_pos_code_daily")))
    .withColumn("curr_subs_status_key_daily_last", last(col("curr_subs_status_key_daily")))
    .withColumn("super_region_name_eng_daily_last", last(col("super_region_name_eng_daily")))
    .withColumn("pp_archetype_daily_last", last(col("pp_archetype_daily")))
    .withColumn("recipient_ind_daily_last", last(col("recipient_ind_daily")).cast("double"))
    .withColumn("donor_ind_daily_last", last(col("donor_ind_daily")).cast("double"))
    .withColumn("last_pp_monthly_fee_daily_last", last(col("last_pp_monthly_fee_daily")).cast("double"))
    .withColumn("last_pp_daily_fee_daily_last", last(col("last_pp_daily_fee_daily")).cast("double"))
    .withColumn("last_pp_data_volume_daily_last", last(col("last_pp_data_volume_daily")).cast("double"))
    .withColumn("last_pp_voice_volume_daily_last", last(col("last_pp_voice_volume_daily")).cast("double"))
    .withColumn("last_pp_archive_ind_daily_last", last(col("last_pp_archive_ind_daily")).cast("double"))
    .withColumn("last_pp_flat_ind_daily_last", last(col("last_pp_flat_ind_daily")).cast("double"))
    .withColumn("last_pp_unlimited_data_daily_last", last(col("last_pp_unlimited_data_daily")).cast("double"))
    .withColumn("days_from_last_pp_started_daily_last", last(col("days_from_last_pp_started_daily")).cast("double"))
    .withColumn("load_date_minus_8", lit(LOAD_DATE_YYYY_MM_DD_8))
    .withColumn("load_date_minus_8", to_date(col("load_date_minus_8"), "yyyy-MM-dd"))
    .withColumn("last_usage_trafficout_daily_last", last(col("last_usage_trafficout_daily")))
    .withColumn("last_usage_trafficout_daily_last", to_date(col("last_usage_trafficout_daily_last"), "yyyy-MM-dd"))
    .withColumn("last_usage_trafficout_daily_last", datediff(col("load_date_minus_8"), col("last_usage_trafficout_daily_last")))
    .withColumn("last_significant_usage_daily_last", last(col("last_significant_usage_daily")))
    .withColumn("last_significant_usage_daily_last", to_date(col("last_significant_usage_daily_last"), "yyyy-MM-dd"))
    .withColumn("last_significant_usage_daily_last", datediff(col("load_date_minus_8"), col("last_significant_usage_daily_last")))
    .drop(col("load_date_minus_8"))
    .withColumn("data_all_mb_daily_last", last(col("data_all_mb_daily")).cast("double"))
    .withColumn("voice_in_sec_daily_last", last(col("voice_in_sec_daily")).cast("double"))
    .withColumn("voice_out_sec_daily_last", last(col("voice_out_sec_daily")).cast("double"))
    .withColumn("voice_in_cnt_daily_last", last(col("voice_in_cnt_daily")).cast("double"))
    .withColumn("voice_out_cnt_daily_last", last(col("voice_out_cnt_daily")).cast("double"))
    .withColumn("balance_daily_last", last(col("balance_daily")).cast("double"))
    .withColumn("bal_neg_flg_daily_last", last(col("bal_neg_flg_daily")).cast("double"))
    .withColumn("bal_low_flg_daily_last", last(col("bal_low_flg_daily")).cast("double"))
    .withColumn("bal_high_flg_daily_last", last(col("bal_high_flg_daily")).cast("double"))
    .withColumn("bal_is_enough_daily_last", last(col("bal_is_enough_daily")).cast("double"))
    .withColumn("recharge_amt_daily_first", first(col("recharge_amt_daily")).cast("double"))
    .withColumn("data_all_mb_daily_first_pos", first_pos(col("data_all_mb_daily")))
    .withColumn("voice_in_sec_daily_first_pos", first_pos(col("voice_in_sec_daily")))
    .withColumn("voice_out_sec_daily_first_pos", first_pos(col("voice_out_sec_daily")))
    .withColumn("uniq_amt_pos_code_daily_first_pos", first_pos(col("uniq_amt_pos_code_daily")))
    .withColumn("data_all_mb_daily_cnt_pos", cnt(col("data_all_mb_daily")))
    .withColumn("voice_in_sec_daily_cnt_pos", cnt(col("voice_in_sec_daily")))
    .withColumn("voice_out_sec_daily_cnt_pos", cnt(col("voice_out_sec_daily")))
    .withColumn("uniq_amt_pos_code_daily_cnt_pos", cnt(col("uniq_amt_pos_code_daily")))
    .withColumn("data_all_mb_daily_ratio_pos_all", ratio_pos(col("data_all_mb_daily")))
    .withColumn("data_all_mb_daily_ratio_pos_since_first", ratio_pos_since_first(col("data_all_mb_daily")))
    .withColumn("data_all_mb_daily_ratio_pos_since_first", when(col("data_all_mb_daily_ratio_pos_since_first") === 0.0, null)
      .otherwise(col("data_all_mb_daily_ratio_pos_since_first")))
    .withColumn("voice_in_sec_daily_ratio_pos_all", ratio_pos(col("voice_in_sec_daily")))
    .withColumn("voice_in_sec_daily_ratio_pos_since_first", ratio_pos_since_first(col("voice_in_sec_daily")))
    .withColumn("voice_in_sec_daily_ratio_pos_since_first", when(col("voice_in_sec_daily_ratio_pos_since_first") === 0.0, null)
      .otherwise(col("voice_in_sec_daily_ratio_pos_since_first")))
    .withColumn("voice_out_sec_daily_ratio_pos_all", ratio_pos(col("voice_out_sec_daily")))
    .withColumn("voice_out_sec_daily_ratio_pos_since_first", ratio_pos_since_first(col("voice_out_sec_daily")))
    .withColumn("voice_out_sec_daily_ratio_pos_since_first", when(col("voice_out_sec_daily_ratio_pos_since_first") === 0.0, null)
      .otherwise(col("voice_out_sec_daily_ratio_pos_since_first")))
    .withColumn("uniq_amt_pos_code_daily_ratio_pos_all", ratio_pos(col("uniq_amt_pos_code_daily")))
    .withColumn("uniq_amt_pos_code_daily_ratio_pos_since_first", ratio_pos_since_first(col("uniq_amt_pos_code_daily")))
    .withColumn("uniq_amt_pos_code_daily_ratio_pos_since_first",
      when(col("uniq_amt_pos_code_daily_ratio_pos_since_first") === 0.0, null)
      .otherwise(col("uniq_amt_pos_code_daily_ratio_pos_since_first")))
    .select(col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("date_key"),
      col("business_type"),
      col("flag_monobrand"),
      col("curr_subs_status_key_daily"),
      col("market_key_daily"),
      col("super_region_name_eng_daily"),
      col("account_type_key_daily"),
      col("business_type_daily"),
      col("ban_type_desc_daily"),
      col("pp_archetype_daily"),
      col("recipient_ind_daily"),
      col("donor_ind_daily"),
      col("lifetime_daily"),
      col("last_usage_trafficout_daily"),
      col("last_significant_usage_daily"),
      col("voice_in_sec_daily"),
      col("voice_out_sec_daily"),
      col("voice_in_cnt_daily"),
      col("voice_out_cnt_daily"),
      col("voice_in_sec_daily_max"),
      col("voice_out_sec_daily_max"),
      col("voice_in_cnt_daily_max"),
      col("voice_out_cnt_daily_max"),
      col("voice_in_sec_daily_avg"),
      col("voice_out_sec_daily_avg"),
      col("voice_in_cnt_daily_avg"),
      col("voice_out_cnt_daily_avg"),
      col("voice_in_sec_daily_std"),
      col("voice_out_sec_daily_std"),
      col("voice_in_cnt_daily_std"),
      col("voice_out_cnt_daily_std"),
      col("data_all_mb_daily"),
      col("data_all_mb_daily_max"),
      col("data_all_mb_daily_avg"),
      col("data_all_mb_daily_std"),
      col("charge_amt_daily"),
      col("charge_amt_daily_max"),
      col("charge_amt_daily_sum"),
      col("charge_amt_daily_avg"),
      col("recharge_amt_daily"),
      col("recharge_amt_daily_max"),
      col("recharge_amt_daily_sum"),
      col("recharge_amt_daily_avg"),
      col("price_plan_key_daily"),
      col("last_pp_monthly_fee_daily"),
      col("last_pp_daily_fee_daily"),
      col("last_pp_data_volume_daily"),
      col("last_pp_voice_volume_daily"),
      col("last_pp_archive_ind_daily"),
      col("last_pp_flat_ind_daily"),
      col("last_pp_unlimited_data_daily"),
      col("days_from_last_pp_started_daily"),
      col("sms_bank_cnt_daily"),
      col("sms_shop_cnt_daily"),
      col("sms_otp_cnt_daily"),
      col("sms_bank_cnt_daily_max"),
      col("sms_shop_cnt_daily_max"),
      col("sms_otp_cnt_daily_max"),
      col("sms_bank_cnt_daily_avg"),
      col("sms_shop_cnt_daily_avg"),
      col("sms_otp_cnt_daily_avg"),
      col("balance_daily"),
      col("bal_neg_flg_daily"),
      col("bal_low_flg_daily"),
      col("bal_high_flg_daily"),
      col("bal_is_enough_daily"),
      col("bal_delta_daily"),
      col("days_cnt_last_daily"),
      col("days_cnt_next_daily"),
      col("payment_amount_daily"),
      col("balance_daily_max"),
      col("bal_neg_flg_daily_max"),
      col("bal_low_flg_daily_max"),
      col("bal_high_flg_daily_max"),
      col("bal_is_enough_daily_max"),
      col("bal_neg_flg_daily_sum"),
      col("bal_low_flg_daily_sum"),
      col("bal_high_flg_daily_sum"),
      col("bal_is_enough_daily_sum"),
      col("balance_daily_avg"),
      col("bal_neg_flg_daily_avg"),
      col("bal_low_flg_daily_avg"),
      col("bal_high_flg_daily_avg"),
      col("bal_is_enough_daily_avg"),
      col("uniq_amt_pos_code_daily"),
      col("uniq_amt_pos_code_daily_max"),
      col("uniq_amt_pos_code_daily_avg"),
      col("curr_subs_status_key_daily_last"),
      col("super_region_name_eng_daily_last"),
      col("pp_archetype_daily_last"),
      col("recipient_ind_daily_last"),
      col("donor_ind_daily_last"),
      col("last_pp_monthly_fee_daily_last"),
      col("last_pp_daily_fee_daily_last"),
      col("last_pp_data_volume_daily_last"),
      col("last_pp_voice_volume_daily_last"),
      col("last_pp_archive_ind_daily_last"),
      col("last_pp_flat_ind_daily_last"),
      col("last_pp_unlimited_data_daily_last"),
      col("days_from_last_pp_started_daily_last"),
      col("last_usage_trafficout_daily_last"),
      col("last_significant_usage_daily_last"),
      col("data_all_mb_daily_last"),
      col("voice_in_sec_daily_last"),
      col("voice_out_sec_daily_last"),
      col("voice_in_cnt_daily_last"),
      col("voice_out_cnt_daily_last"),
      col("balance_daily_last"),
      col("bal_neg_flg_daily_last"),
      col("bal_low_flg_daily_last"),
      col("bal_high_flg_daily_last"),
      col("bal_is_enough_daily_last"),
      col("recharge_amt_daily_first"),
      col("data_all_mb_daily_first_pos"),
      col("voice_in_sec_daily_first_pos"),
      col("voice_out_sec_daily_first_pos"),
      col("uniq_amt_pos_code_daily_first_pos"),
      col("data_all_mb_daily_cnt_pos"),
      col("voice_in_sec_daily_cnt_pos"),
      col("voice_out_sec_daily_cnt_pos"),
      col("uniq_amt_pos_code_daily_cnt_pos"),
      col("data_all_mb_daily_ratio_pos_all"),
      col("data_all_mb_daily_ratio_pos_since_first"),
      col("voice_in_sec_daily_ratio_pos_all"),
      col("voice_in_sec_daily_ratio_pos_since_first"),
      col("voice_out_sec_daily_ratio_pos_all"),
      col("voice_out_sec_daily_ratio_pos_since_first"),
      col("uniq_amt_pos_code_daily_ratio_pos_all"),
      col("uniq_amt_pos_code_daily_ratio_pos_since_first"),
      col("report_dt"))

  res
    .repartition(1)
    .write
    .format("orc")
    .mode("overwrite")
    .insertInto("nba_engine.core_clients_model_sample")

  spark.catalog.clearCache()
  spark.stop()

}
