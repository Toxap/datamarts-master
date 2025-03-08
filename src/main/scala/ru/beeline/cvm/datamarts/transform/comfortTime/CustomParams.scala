package ru.beeline.cvm.datamarts.transform.comfortTime

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
  val SCORE_DATE = inputFormat.format(LOAD_DATE)

  val TableStgGeoAll = getAppConf(jobConfigKey, "tableStgGeoAll")
  val TableAggSubsProfile = getAppConf(jobConfigKey, "tableAggSubsProfile")
  val TableTrefMarketInfo = getAppConf(jobConfigKey, "tableTrefMarketInfo")
  val TableNbaCtnTzPrev = getAppConf(jobConfigKey, "tableNbaCtnTzPrev")
  val TableNbaAvailableTime = getAppConf(jobConfigKey, "tableNbaAvailableTime")

}
