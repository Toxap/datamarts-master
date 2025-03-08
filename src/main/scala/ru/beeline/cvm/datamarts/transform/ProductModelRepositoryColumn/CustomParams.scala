package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumn

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val TablePredSaleProductAll: String = getAppConf(jobConfigKey, "tablePredSaleProductAll")
  val TableProductModelEffect: String = getAppConf(jobConfigKey, "tableProductModelEffect")
  val TableProductChannelCost: String = getAppConf(jobConfigKey, "tableProductChannelCost")
  val TableProductTreshhold: String = getAppConf(jobConfigKey, "tableProductTreshhold")
  val TableProductModelRepositoryColumn: String = getAppConf(jobConfigKey, "tableProductModelRepositoryColumn")

}
