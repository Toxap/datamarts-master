package ru.beeline.cvm.datamarts.transform.SubPreset

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val SCORE_DATE_4 = inputFormat.format(LOAD_DATE.minusDays(4))
  val SCORE_DATE_3 = inputFormat.format(LOAD_DATE.minusDays(3))
  val SCORE_DATE_2 = inputFormat.format(LOAD_DATE.minusDays(2))
  val SCORE_DATE = inputFormat.format(LOAD_DATE.minusDays(1))
  val SCORE_DATE_MINUS_3_MONTH = inputFormat.format(LOAD_DATE.minusMonths(3))

  val TableDimSocParameter = getAppConf(jobConfigKey, "tableDimSocParameter")
  val TableDmMobileSubscriber = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val TableVPpConstructorParam = getAppConf(jobConfigKey, "tableVPpConstructorParam")
  val TableServiceAgreementPub = getAppConf(jobConfigKey, "tableServiceAgreementPub")
  val TableAggCtnBasePubD = getAppConf(jobConfigKey, "tableAggCtnBasePubD")
  val TableSubPreset = getAppConf(jobConfigKey, "tableSubPreset")
  val TableDmMobileSubscriberUsageDaily = getAppConf(jobConfigKey, "tableDmMobileSubscriberUsageDaily")
  val TableCaMainSubscriber = getAppConf(jobConfigKey, "tableCaMainSubscriber")

}
