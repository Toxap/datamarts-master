package ru.beeline.cvm.datamarts.transform.prolongModelCorrected

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val SCORE_DATE = inputFormat.format(LOAD_DATE)

  val numPartiton = getAppConf(jobConfigKey, "coalesce").toInt
  val TableProlongOutput = getAppConf(jobConfigKey, "tableProlongOutput")
  val TableProlongOffersAvailable = getAppConf(jobConfigKey, "tableProlongOffersAvailable")
  val TableProlongCustFeatures = getAppConf(jobConfigKey, "tableProlongCustFeatures")
  val TableProlongOutputCorrected = getAppConf(jobConfigKey, "tableProlongOutputCorrected")
  val TableProlongOffersAvailableCorrected = getAppConf(jobConfigKey, "tableProlongOffersAvailableCorrected")
  val TableProlongCustFeaturesCorrected = getAppConf(jobConfigKey, "tableProlongCustFeaturesCorrected")

}
