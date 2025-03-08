package ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.{DayOfWeek, LocalDate}
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val LOAD_DATE_YYYY_MM_DD: String = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)
  private val LOAD_DATE_START_WEEK: LocalDate = LOAD_DATE.`with`(DayOfWeek.MONDAY)

  val LOAD_DATE_START_WEEK_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE_START_WEEK)
  val LOAD_DATE_START_WEEK_YYYY_MM_DD_13W: String = inputFormat.format(LOAD_DATE_START_WEEK.minusWeeks(13))
  val LOAD_DATE_YYYY_MM_DD_1D: String = inputFormat.format(LOAD_DATE.minusDays(1))
  val LOAD_DATE_YYYY_MM_DD_2D: String = inputFormat.format(LOAD_DATE.minusDays(2))
  val LOAD_DATE_YYYY_MM_DD_86D: String = inputFormat.format(LOAD_DATE.minusDays(86))

  val TableDmMobileSubscriber: String = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val TableDimSocParameter: String = getAppConf(jobConfigKey, "tableDimSocParameter")
  val TableAggCtnBasePubD: String = getAppConf(jobConfigKey, "tableAggCtnBasePubD")
  val TableServiceAgreementPub: String = getAppConf(jobConfigKey, "tableServiceAgreementPub")
  val TableVPpConstructorParam: String = getAppConf(jobConfigKey, "tableVPpConstructorParam")
  val TableStgFamilyService: String = getAppConf(jobConfigKey, "tableStgFamilyService")
  val TableDmMobileSubscriberUsageDaily: String = getAppConf(jobConfigKey, "tableDmMobileSubscriberUsageDaily")
  val TableDimUpsellPresetConstruct: String = getAppConf(jobConfigKey, "tableDimUpsellPresetConstruct")
  val TableDimUpsellPresetConstruct5Offers: String = getAppConf(jobConfigKey, "tableDimUpsellPresetConstruct5Offers")


}
