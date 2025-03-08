package ru.beeline.cvm.datamarts.transform.productCategoryProfile

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
  val SCORE_DATE = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableModelScore = getAppConf(jobConfigKey, "tableModelScore")
  val TableProductTreshholdModelSource = getAppConf(jobConfigKey, "tableProductTreshholdModelSource")
  val TableAanaaMvpCategorization = getAppConf(jobConfigKey, "tableAanaaMvpCategorization")
  val TableAnaMvpCategorization = getAppConf(jobConfigKey, "tableAnaMvpCategorization")
  val TableProductCategoryProfile = getAppConf(jobConfigKey, "tableProductCategoryProfile")


}
