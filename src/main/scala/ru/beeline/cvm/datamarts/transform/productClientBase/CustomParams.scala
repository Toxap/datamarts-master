package ru.beeline.cvm.datamarts.transform.productClientBase

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
  val SCORE_DATE = inputFormat.format(LOAD_DATE)
  val FIRST_DAY_OF_MONTH_YYYY_MM_01 = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableAggSubsProfilePubD = getAppConf(jobConfigKey, "tableAggSubsProfilePubD")
  val TableProductClientBase = getAppConf(jobConfigKey, "tableProductClientBase")


}
