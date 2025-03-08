package ru.beeline.cvm.datamarts.transform.winbackDataset

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters.lastDayOfMonth

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  private val outputFormatYYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val outputFormat01 = DateTimeFormatter.ofPattern("yyyy-MM-01")
  private val outputFormat14 = DateTimeFormatter.ofPattern("yyyy-MM-14")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val SCORE_DATE: String = inputFormat.format(LOAD_DATE)
  val SCORE_DATE_12: String = inputFormat.format(LOAD_DATE) + " 12:00:00"
  val LOAD_DATE_YYYY_MM_01: String = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val LOAD_DATE_PYYYYMM: String = "P" + outputFormat.format(LOAD_DATE)

  val LOAD_DATE_YYYY_MM_DD_2M: String = inputFormat.format(LOAD_DATE.minusMonths(2))
  val LOAD_DATE_YYYY_MM_DD_4M: String = inputFormat.format(LOAD_DATE.minusMonths(4))


  val LOAD_DATE_PYYYYMM_2M: String = "P" + outputFormat.format(LOAD_DATE.minusMonths(2))
  val LOAD_DATE_PYYYYMM_4M: String = "P" + outputFormat.format(LOAD_DATE.minusMonths(4))

  val LOAD_DATE_YYYYY_MM_01_1M: String = outputFormat01.format(LOAD_DATE.minusMonths(1))
  val LOAD_DATE_YYYYY_MM_14_1M: String = outputFormat14.format(LOAD_DATE.minusMonths(1))

  val LOAD_DATE_YYYY_MM_LAST_DAY_2M: String = inputFormat.format(LOAD_DATE.`with`(lastDayOfMonth()).minusMonths(2))

  val TableAggCtnUsagePubD: String = getAppConf(jobConfigKey, "tableAggCtnUsagePubD")
  val TableAggCtnSmsCatPubM: String = getAppConf(jobConfigKey, "tableAggCtnSmsCatPubM")
  val TableAggCtnBalancePubM: String = getAppConf(jobConfigKey, "tableAggCtnBalancePubM")
  val TableFctActClustSubsMPub: String = getAppConf(jobConfigKey, "tableFctActClustSubsMPub")
  val TableNbaCustomersPub: String = getAppConf(jobConfigKey, "tableNbaCustomersPub")
  val TableWinbackDataset: String = getAppConf(jobConfigKey, "tableWinbackDataset")

  val MIN_FEAT_LAG = 2
  val MAX_FEAT_LAG = 4
  val TARGET_LEAD_DAYS = 7
  val TARGET_RECHARGES_THRESHOLD_RUB = 100
  val PREV_MONTH_FEAT_START_DAY = 1
  val PREV_MONTH_FEAT_END_DAY = 14
  val FEATURES_AGG_CTN_SMS_CAT_WITH_LAGS = Map("normalized_sms_bank_cnt_m" -> (MIN_FEAT_LAG, MAX_FEAT_LAG))
  val FEATURES_AGG_CTN_SMS_CAT_NULL_REPLACE = Map("normalized_sms_bank_cnt_m" -> 0)

  val FEATURES_AGG_CTN_USAGE_WITH_LAGS = Map(
    "internet_active_flg" -> (PREV_MONTH_FEAT_START_DAY, PREV_MONTH_FEAT_END_DAY),
    "voice_out_active_flg" -> (PREV_MONTH_FEAT_START_DAY, PREV_MONTH_FEAT_END_DAY),
    "voice_input_active_flg" -> (PREV_MONTH_FEAT_START_DAY, PREV_MONTH_FEAT_END_DAY))

  val FEATURES_AGG_CTN_USAGE_NULL_REPLACE = Map(
    "internet_active_flg" -> 0,
    "voice_out_active_flg" -> 0,
    "voice_input_active_flg" -> 0)

  val FEATURES_AGG_CTN_BALANCE_WITH_LAGS = Map(
    "normalized_bal_neg_cnt_m" -> (MIN_FEAT_LAG, MAX_FEAT_LAG))

  val FEATURES_AGG_CTN_BALANCE_NULL_REPLACES = Map(
    "normalized_bal_neg_cnt_m" -> 0)

  val FEATURES_FCT_ACT_CLUST_SUBS_M_WITH_LAGS = Map(
    "normalized_total_recharge_amt_rur" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_cnt_recharge" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "bs_data_rate" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "bs_voice_rate" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_total_rev" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_direct_rev" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_total_contact" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_cnt_voice_day" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_cnt_data_day" -> (MIN_FEAT_LAG, MAX_FEAT_LAG),
    "normalized_mou_out_bln" -> (MIN_FEAT_LAG, MAX_FEAT_LAG))

  val FEATURES_WOUT_AGG_WITH_LAGS = Map(
    "cluster_num" -> (MIN_FEAT_LAG, None),
    "is_smartphone" -> (MIN_FEAT_LAG, None),
    "permanent_aab" -> (MIN_FEAT_LAG, None))

  val FEATURES_FCT_ACT_CLUST_SUBS_M_NULL_REPLACES = Map(
    "normalized_total_recharge_amt_rur" -> 0,
    "normalized_cnt_recharge" -> 0,
    "bs_data_rate" -> 0,
    "bs_voice_rate" -> 0,
    "normalized_total_rev" -> 0,
    "normalized_direct_rev" -> 0,
    "normalized_total_contact" -> 0,
    "normalized_cnt_voice_day" -> 0,
    "normalized_cnt_data_day" -> 0,
    "normalized_mou_out_bln" -> 0,
    "cluster_num" -> 10,
    "is_smartphone" -> 0,
    "permanent_aab" -> 0)

}
