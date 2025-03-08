package ru.beeline.cvm.datamarts.transform.commPolit

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val SCORE_DATE = inputFormat.format(LOAD_DATE.minusDays(1))
  val SCORE_DATE_4 = inputFormat.format(LOAD_DATE.minusDays(4))
  val FIRST_DAY_OF_MONTH_YYYY_MM_01 = outputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val FIRST_DAY_OF_MONTH_YYYY_MM_01_12M = outputFormat.format(LOAD_DATE.minusMonths(12).withDayOfMonth(1))
  val LOAD_DATE_PYYYYMM_2M = "P" + outputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(2))
  val LOAD_DATE_PYYYYMM_6M = "P" + outputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(6))
  val LOAD_DATE_YYYYMM_2M = outputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(2))
  val LOAD_DATE_YYYYMM_6M = outputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(6))

  val TableCmsMembers = getAppConf(jobConfigKey, "tableCmsMembers")
  val TableFctActClustSubsMPub = getAppConf(jobConfigKey, "tableFctActClustSubsMPub")
  val TableDmMobileSubscriber = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val TableTCampsLastPart = getAppConf(jobConfigKey, "tableTCampsLastPart")
  val TableAggOutboundDetailHistory = getAppConf(jobConfigKey, "tableAggOutboundDetailHistory")
  val TableCommPoliteSample = getAppConf(jobConfigKey, "tableCommPoliteSample")

}
