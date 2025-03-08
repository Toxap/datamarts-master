package ru.beeline.cvm.datamarts.transform.tableModelScore

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
  val SCORE_DATE = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TablePredSaleProduct1 = getAppConf(jobConfigKey, "tablePredSaleProduct1")
  val TablePredSaleProduct2 = getAppConf(jobConfigKey, "tablePredSaleProduct2")
  val TablePredSaleProduct3 = getAppConf(jobConfigKey, "tablePredSaleProduct3")
  val TablePredSaleProduct4 = getAppConf(jobConfigKey, "tablePredSaleProduct4")
  val TablePredSaleProduct6 = getAppConf(jobConfigKey, "tablePredSaleProduct6")
  val TablePredSaleProduct8 = getAppConf(jobConfigKey, "tablePredSaleProduct8")
  val TablePredSaleProduct9 = getAppConf(jobConfigKey, "tablePredSaleProduct9")
  val TablePredSaleProduct10 = getAppConf(jobConfigKey, "tablePredSaleProduct10")
  val TablePredSaleProduct11 = getAppConf(jobConfigKey, "tablePredSaleProduct11")
  val TablePredSaleProduct12 = getAppConf(jobConfigKey, "tablePredSaleProduct12")
  val TablePredSaleProduct13 = getAppConf(jobConfigKey, "tablePredSaleProduct13")
  val TablePredSaleProduct14 = getAppConf(jobConfigKey, "tablePredSaleProduct14")
  val TablePredSaleProduct15 = getAppConf(jobConfigKey, "tablePredSaleProduct15")
  val TablePredSaleProduct16 = getAppConf(jobConfigKey, "tablePredSaleProduct16")
  val TablePredSaleProduct18 = getAppConf(jobConfigKey, "tablePredSaleProduct18")
  val TablePredSaleProduct23 = getAppConf(jobConfigKey, "tablePredSaleProduct23")
  val TableModelScore = getAppConf(jobConfigKey, "tableModelScore")


}
