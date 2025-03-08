package ru.beeline.cvm.datamarts.transform.callCyclesHist

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit  {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val startMonthFormat = DateTimeFormatter.ofPattern("yyyy-MM-01")

  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val REPORT_DATE_MONTH_BEGIN = LOAD_DATE.plusDays(4).format(startMonthFormat)
  val REPORT_DATE = LOAD_DATE.plusDays(4).format(inputFormat)
  val REPORT_DATE_MINUS_DAY = LOAD_DATE.plusDays(3).format(inputFormat)
  val REPORT_DATE_MINUS_300_START_MONTHS = LOAD_DATE.plusDays(4).minusDays(300).format(startMonthFormat)
  val REPORT_DATE_MINUS_300 = LOAD_DATE.plusDays(4).minusDays(300).format(inputFormat)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableDimCtnBan: String = getAppConf(jobConfigKey, "tableDimCtnBanCallCycle")
  val TableCallCycles: String = getAppConf(jobConfigKey, "tableCallCycles")
  val TableCallCyclesHist: String = getAppConf(jobConfigKey, "tableCallCyclesHist")

}
