package ru.beeline.cvm.datamarts.transform.ProductNonmodelRepositoryColumn

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val TableProductAnalytPredSaleSource: String = getAppConf(jobConfigKey, "tableProductAnalytPredSaleSource")
  val TableProductChannelCost: String = getAppConf(jobConfigKey, "tableProductChannelCost")
  val TableProductModelEffectSource: String = getAppConf(jobConfigKey, "tableProductModelEffectSource")
  val TableProductNonmodelRepositoryColumn: String = getAppConf(jobConfigKey, "tableProductNonmodelRepositoryColumn")

}
