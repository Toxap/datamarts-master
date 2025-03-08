package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters.lastDayOfMonth

class CustomParams extends CustomParamsInit {


  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")

  val partitionField = getAppConf(jobConfigKey, "partitionField")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val SCORE_DATE = inputFormat.format(LOAD_DATE)
  val SCORE_DATE_1 = inputFormat.format(LOAD_DATE.plusDays(1))
  val FIRST_DAY_OF_MONTH_PYYYYMM = "P" + outputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val FIRST_DAY_OF_MONTH_YYYY_MM_01 = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val LOAD_DATE_PYYYYMM_2M = "P" + outputFormat.format(LOAD_DATE.minusMonths(2))
  val LOAD_DATE_PYYYYMM_7M = "P" + outputFormat.format(LOAD_DATE.minusMonths(7))
  val FIRST_DAY_OF_MONTH_YYYY_MM_01_6M = inputFormat.format(LOAD_DATE.minusMonths(6).withDayOfMonth(1))
  val FIRST_DAY_OF_MONTH_YYYY_MM_01_2M = inputFormat.format(LOAD_DATE.minusMonths(2).withDayOfMonth(1))
  val LAST_DAY_OF_MONTH_YYYY_MM_DD_1M = inputFormat.format(LOAD_DATE.minusMonths(1).`with`(lastDayOfMonth()))
  val CURRENT_DATE = LocalDate.now().toString()

  val TableNbaCustomers = getAppConf(jobConfigKey, "tableNbaCustomersPub")
  val TableFctRtcMonthlyPub = getAppConf(jobConfigKey, "tableFctRtcMonthlyPub")
  val TableVoiceAllPub = getAppConf(jobConfigKey, "tableVoiceAllPub")
  val TableDataAllPub = getAppConf(jobConfigKey, "tableDataAllPub")
  val TableFamilyService = getAppConf(jobConfigKey, "tableFamilyService")
  val TableFctActClustSubsMPub = getAppConf(jobConfigKey, "tableFctActClustSubsMPub")
  val TableDimBanPub = getAppConf(jobConfigKey, "tableDimBanPub")
  val TableDimSocParameter = getAppConf(jobConfigKey, "tableDimSocParameter")
  val TableDimPpConstructorSoc = getAppConf(jobConfigKey, "tableDimPpConstructorSoc")
  val TableDimDic = getAppConf(jobConfigKey, "tableDimDic")
  val TableDimDicLink = getAppConf(jobConfigKey, "tableDimDicLink")
  val TableFctServiceSubscriptionPub = getAppConf(jobConfigKey, "tableFctServiceSubscriptionPub")
  val TableAdsDataset = getAppConf(jobConfigKey, "tableAdsDataset")
  val TableDimSubscriberPub = getAppConf(jobConfigKey, "tableDimSubscriberPub")
  val TableServiceAgreementPub = getAppConf(jobConfigKey, "tableServiceAgreementPub")
}
