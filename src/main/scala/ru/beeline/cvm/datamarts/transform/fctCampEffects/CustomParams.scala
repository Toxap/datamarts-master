package ru.beeline.cvm.datamarts.transform.fctCampEffects

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val SCORE_DATE = inputFormat.format(LOAD_DATE.minusDays(1))
  val SCORE_DATE_PLUS_1_MONTH = inputFormat.format(LOAD_DATE.plusMonths(1))

  val TableCampLibrary = getAppConf(jobConfigKey, "tableCampLibrary")
  val TableWave = getAppConf(jobConfigKey, "tableWave")
  val TableCampToEventType = getAppConf(jobConfigKey, "tableCampToEventType")
  val TableCumulativeEffCampaing = getAppConf(jobConfigKey, "tableCumulativeEffCampaing")
  val TableTCampsLastPart = getAppConf(jobConfigKey, "tableTCampsLastPart")
  val TableDimDic = getAppConf(jobConfigKey, "tableDimDic")
  val TableFctAvgSaleIsnUseCase = getAppConf(jobConfigKey, "tableFctAvgSaleIsnUseCase")
  val TableFctDetailCampEffect = getAppConf(jobConfigKey, "tableFctDetailCampEffect")
  val TableFctAvgCampEffect = getAppConf(jobConfigKey, "tableFctAvgCampEffect")

}
