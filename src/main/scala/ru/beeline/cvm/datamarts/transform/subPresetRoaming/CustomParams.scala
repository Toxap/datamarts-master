package ru.beeline.cvm.datamarts.transform.subPresetRoaming

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableDmMobileSubscriberUsageDayly: String = getAppConf(jobConfigKey, "tableDMMOBILESUBSCRIBERUSAGEDAILY")
  val TableDmMobileSubscriber: String = getAppConf(jobConfigKey, "tableDMMOBILESUBSCRIBER")
  val TableSubPresetRoaming: String = getAppConf(jobConfigKey, "tableSubPresetRoaming")

  val SCORE_DATE_4 = inputFormat.format(LOAD_DATE.minusDays(4))
  val SCORE_DATE_MINUS_ONE_YEAR = inputFormat.format(LOAD_DATE.minusYears(1))
}
