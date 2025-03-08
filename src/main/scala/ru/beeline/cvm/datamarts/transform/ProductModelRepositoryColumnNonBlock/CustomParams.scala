package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumnNonBlock

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val TableProductModelRepositoryColumn: String = getAppConf(jobConfigKey, "tableProductModelRepositoryColumn")
  val TableProductModelRepositoryColumnNonBlock: String = getAppConf(jobConfigKey, "tableProductModelRepositoryColumnNonBlock")

}
