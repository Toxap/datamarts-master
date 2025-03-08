package ru.beeline.cvm.datamarts.transform.coreClientsTarget

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit{
  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val LOAD_DATE_YYYY_MM_DD_0: String = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val LOAD_DATE_YYYY_MM_DD_minus3: String = inputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(3))
  val LOAD_DATE_YYYY_MM_DD_minus2: String = inputFormat.format(LOAD_DATE.withDayOfMonth(1).minusMonths(2))


  val tableDmMobileSubscriber = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val tableCoreModelScoring2 = getAppConf(jobConfigKey, "tableCoreModelScoring2")
  val tableDmMobileSubscriberCoreSegment = getAppConf(jobConfigKey, "tableDmMobileSubscriberCoreSegment")


}
