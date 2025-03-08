package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class AntiDownSellDataset extends DimAllSoc {

  val nbaCustomers = if (SCORE_DATE == CURRENT_DATE) getDimSubscriberPub else getNbaCustomer
  val fctRtcMonthlyPub = getFctRtcMonthlyPub
  val voiceAllPub = getVoiceAllPub
  val dataAllPub = getDataAllPub
  val familyService = getFamilyService
  val fctActClustSubsMPub = getFctActClustSubsMPub
  val dimBan = getDimBan
  val fillNonConstructorSoc = createUsualPpContent
  val fillConstructorSoc = createConstructorSocContent
      .filter(col("start_time_key") <= lit(SCORE_DATE))
      .filter(col("end_time_key") > lit(SCORE_DATE))
  val initActivationCoreAABDate = getInitActivationAndCoreAABDate
  val serviceAgreement = getServiceAgreementPub
  val serviceAgreementSocs = calculateConstructorSoc


  val subsNotConstructorSoc = nbaCustomers.join(fillNonConstructorSoc, Seq("soc_code", "market_key")).as("a")

  val subsNotConstructorSocA = subsNotConstructorSoc
    .join(serviceAgreement
              .filter(col("service_class") === "PP")
              .filter(col("sys_update_date_corr") > lit(SCORE_DATE)).as("b"),
          Seq("subs_key", "ban_key"))
    .filter(col("a.soc_code") === col("b.soc_code"))
    .withColumn("r", row_number().over(Window.partitionBy("subs_key", "ban_key").orderBy(col("sys_creation_date"))))
    .filter(col("r") === 1)
    .select(
        col("subs_key"),
        col("ban_key"),
        col("a.soc_code"),
        col("constructor_id"),
        col("market_key"),
        col("business_name_product_info"),
        col("product_id_product_info"),
        col("product_name_product_info"),
        col("product_category_product_info"),
        col("monthly_fee"),
        col("daily_fee"),
        col("count_voice_min"),
        col("count_data_gb"),
        col("count_sms"),
        col("count_available_family_ctn"),
        col("archive_ind"),
        col("sys_creation_date").as("effective_date_current"))
        .distinct()


  val subsWithConstructorSoc = nbaCustomers.filter(col("soc_code").isin(listConstructorSoc: _*))
    .join(serviceAgreementSocs.filter(col("sys_update_date_corr") >= lit(SCORE_DATE)),
          Seq("subs_key", "ban_key"), "left")

  val subsWithConstructorSocA = subsWithConstructorSoc
    .join(fillConstructorSoc, Seq("soc_code", "market_key", "data_soc", "voice_soc"))

  val countConstSubs = subsWithConstructorSocA
    .groupBy("subs_key", "ban_key", "soc_code", "market_key", "constructor_id", "start_time_key", "end_time_key")
    .agg(count(lit(1)).as("cnt"), max(col("sys_creation_date")).as("max_sys_creation_date"))
  val countConstSubsMatch = countConstSubs
    .groupBy("subs_key", "ban_key", "soc_code", "market_key")
    .agg(sum(when(col("cnt") === 1, 1).otherwise(0)).as("double_match"))
  val countConstSubsDoubleMatch = countConstSubs
    .join(countConstSubsMatch.filter(col("double_match") === 1),  Seq("subs_key", "ban_key", "soc_code", "market_key"))
    .filter(col("cnt") === 1)

  val resSubsWithTK = countConstSubsDoubleMatch
    .filter(col("double_match") === 1)
    .join(fillConstructorSoc, Seq("soc_code", "market_key", "constructor_id", "start_time_key", "end_time_key"))
    .select(
        col("subs_key"),
        col("ban_key"),
        col("soc_code"),
        col("constructor_id"),
        col("market_key"),
        col("business_name_product_info"),
        col("product_id_product_info"),
        col("product_name_product_info"),
        col("product_category_product_info"),
        col("monthly_fee"),
        col("daily_fee"),
        col("count_voice_min"),
        col("count_data_gb"),
        col("count_sms"),
        col("count_available_family_ctn"),
        col("archive_ind"),
        col("max_sys_creation_date").as("effective_date_current"))
    .distinct()


    val res = subsNotConstructorSocA.unionByName(resSubsWithTK).distinct()
    .join(fctRtcMonthlyPub, Seq("subs_key", "ban_key"), "left")
    .join(voiceAllPub, Seq("subs_key", "ban_key"), "left")
    .join(dataAllPub, Seq("subs_key", "ban_key"), "left")
    .join(fctActClustSubsMPub, Seq("subs_key", "ban_key"), "left")
    .join(dimBan, Seq("ban_key"), "left")
    .join(familyService, Seq("subs_key"), "left")
    .join(initActivationCoreAABDate, Seq("subs_key", "ban_key"), "left")
    .withColumn("days_on_pp", datediff(lit(SCORE_DATE), col("effective_date_current")))
    .withColumn("pp_fee_current", greatest(
      col("monthly_fee"), col("daily_fee") * 30))
    .withColumn("pp_fee_current", when(
      col("monthly_fee") - col("daily_fee") * 30 < -300,
      col("monthly_fee")).otherwise(col("pp_fee_current")))
    .withColumn("_1_r_data_usage_1_6", col("_1_data_all_mb") / 1024 / col("count_data_gb"))
    .withColumn("_2_r_data_usage_1_6", col("_2_data_all_mb") / 1024 / col("count_data_gb"))
    .withColumn("_3_r_data_usage_1_6", col("_3_data_all_mb") / 1024 / col("count_data_gb"))
    .withColumn("_4_r_data_usage_1_6", col("_4_data_all_mb") / 1024 / col("count_data_gb"))
    .withColumn("_5_r_data_usage_1_6", col("_5_data_all_mb") / 1024 / col("count_data_gb"))
    .withColumn("_6_r_data_usage_1_6", col("_6_data_all_mb") / 1024 / col("count_data_gb"))

    .withColumn("_1_r_voice_usage_1_6", col("_1_voice_out_sec") / 60 / col("count_voice_min"))
    .withColumn("_2_r_voice_usage_1_6", col("_2_voice_out_sec") / 60 / col("count_voice_min"))
    .withColumn("_3_r_voice_usage_1_6", col("_3_voice_out_sec") / 60 / col("count_voice_min"))
    .withColumn("_4_r_voice_usage_1_6", col("_4_voice_out_sec") / 60 / col("count_voice_min"))
    .withColumn("_5_r_voice_usage_1_6", col("_5_voice_out_sec") / 60 / col("count_voice_min"))
    .withColumn("_6_r_voice_usage_1_6", col("_6_voice_out_sec") / 60 / col("count_voice_min"))

    .withColumn("_2_r_arpu_to_pp_2_7", col("_2_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("_3_r_arpu_to_pp_2_7", col("_3_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("_4_r_arpu_to_pp_2_7", col("_4_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("_5_r_arpu_to_pp_2_7", col("_5_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("_6_r_arpu_to_pp_2_7", col("_6_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("_7_r_arpu_to_pp_2_7", col("_7_arpu_amt_r") / col("pp_fee_current"))
    .withColumn("r_data_usage_1_6_max", greatest(col("_1_r_data_usage_1_6"),
      col("_2_r_data_usage_1_6"),
      col("_3_r_data_usage_1_6"),
      col("_4_r_data_usage_1_6"),
      col("_5_r_data_usage_1_6"),
      col("_6_r_data_usage_1_6")))
    .withColumn("r_data_usage_1_6_min", least(col("_1_r_data_usage_1_6"),
      col("_2_r_data_usage_1_6"),
      col("_3_r_data_usage_1_6"),
      col("_4_r_data_usage_1_6"),
      col("_5_r_data_usage_1_6"),
      col("_6_r_data_usage_1_6")))
    .withColumn("r_data_usage_1_6_mean", rowMean(marksColumnsData))


    .withColumn("r_voice_usage_1_6_max", greatest(col("_1_r_voice_usage_1_6"),
      col("_2_r_voice_usage_1_6"),
      col("_3_r_voice_usage_1_6"),
      col("_4_r_voice_usage_1_6"),
      col("_5_r_voice_usage_1_6"),
      col("_6_r_voice_usage_1_6")))
    .withColumn("r_voice_usage_1_6_min", least(col("_1_r_voice_usage_1_6"),
      col("_2_r_voice_usage_1_6"),
      col("_3_r_voice_usage_1_6"),
      col("_4_r_voice_usage_1_6"),
      col("_5_r_voice_usage_1_6"),
      col("_6_r_voice_usage_1_6")))
    .withColumn("r_voice_usage_1_6_mean", rowMean(marksColumnsVoice))


    .withColumn("r_arpu_to_pp_2_7_max", greatest(col("_2_r_arpu_to_pp_2_7"),
      col("_3_r_arpu_to_pp_2_7"),
      col("_4_r_arpu_to_pp_2_7"),
      col("_5_r_arpu_to_pp_2_7"),
      col("_6_r_arpu_to_pp_2_7"),
      col("_7_r_arpu_to_pp_2_7")))
    .withColumn("r_arpu_to_pp_2_7_min", least(col("_2_r_arpu_to_pp_2_7"),
      col("_3_r_arpu_to_pp_2_7"),
      col("_4_r_arpu_to_pp_2_7"),
      col("_5_r_arpu_to_pp_2_7"),
      col("_6_r_arpu_to_pp_2_7"),
      col("_7_r_arpu_to_pp_2_7")))
    .withColumn("r_arpu_to_pp_2_7_mean", rowMean(marksColumnsArpu))
    .select(
      col("subs_key"),
      col("ban_key"),
      nbaCustomers.col("market_key"),
      col("soc_code").as("soc_code_current"),
      col("constructor_id").as("constructor_id_current"),
      col("business_name_product_info").as("business_name_current"),
      col("product_id_product_info").as("product_id_current"),
      col("product_name_product_info").as("product_name_current"),
      col("product_category_product_info").as("product_category_current"),
      col("archive_ind").as("archive_ind_current"),
      col("monthly_fee").as("monthly_fee_current"),
      col("daily_fee").as("daily_fee_current"),
      col("count_voice_min").cast("double").as("count_voice_min_current"),
      col("count_data_gb").cast("double").as("count_data_gb_current"),
      col("count_sms").as("count_sms_current"),
      col("count_available_family_ctn").as("count_available_family_ctn_current"),
      col("effective_date_current").as("effective_date_current"),
      col("_2_data_h_rev"),
      col("_2_total_contact"),
      col("bs_cnt_total_max"),
      col("total_recharge_amt_rur_mean"),
      col("balance_amt_end_mean"),
      col("balance_amt_end_min"),
      col("cnt_recharge_mean"),
      col("cnt_recharge_max"),
      col("data_h_rev_mean"),
      col("data_h_rev_min"),
      col("data_h_rev_max"),
      col("oc_rev_mean"),
      col("contribution_margin_min"),
      col("contact_mgf_max"),
      datediff(lit(FIRST_DAY_OF_MONTH_YYYY_MM_01_2M), col("init_activation_date")).as("init_activation_date_delta"),
      datediff(lit(FIRST_DAY_OF_MONTH_YYYY_MM_01_2M), col("core_aab_first_month")).as("core_aab_first_month_delta"),
      col("_1_voice_out_sec"),
      col("_1_voice_out_cnt"),
      col("_1_voice_intercity_in_cnt"),
      col("voice_out_sec_mean"),
      col("voice_out_cnt_min"),
      col("voice_out_cnt_max"),
      col("voice_in_cnt_min"),
      col("_1_data_all_mb"),
      col("_1_data_4g_mb"),
      col("_1_data_all_mb_day_usage_cnt"),
      col("data_all_mb_mean"),
      col("data_all_mb_min"),
      col("data_all_mb_max"),
      col("data_4g_mb_mean"),
      col("data_4g_mb_max"),
      col("_2_cnt_recipients"),
      col("cnt_recipients_mean"),
      col("customer_age"),
      col("days_on_pp"),
      col("pp_fee_current"),
      col("_1_r_data_usage_1_6"),
      col("_1_r_voice_usage_1_6"),
      col("_2_r_arpu_to_pp_2_7"),
      col("r_data_usage_1_6_max"),
      col("r_data_usage_1_6_min"),
      col("r_data_usage_1_6_mean"),
      col("r_voice_usage_1_6_max"),
      col("r_voice_usage_1_6_min"),
      col("r_voice_usage_1_6_mean"),
      col("r_arpu_to_pp_2_7_max"),
      col("r_arpu_to_pp_2_7_min"),
      col("r_arpu_to_pp_2_7_mean"),
      lit(current_date()).as("report_dt"),
      lit(SCORE_DATE).as("time_key"))
      .distinct()

}
