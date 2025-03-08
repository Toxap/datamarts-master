package ru.beeline.cvm.datamarts.transform.pilotsOutb

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
  val SCORE_DATE = inputFormat.format(LOAD_DATE)

  val TableAggNbaPilot = getAppConf(jobConfigKey, "tableAggNbaPilot")
  val TableCvmFlags = getAppConf(jobConfigKey, "tableCvmFlags")
  val TableCvmNbaOutboundAddInfo = getAppConf(jobConfigKey, "tableCvmNbaOutboundAddinfo")

}
