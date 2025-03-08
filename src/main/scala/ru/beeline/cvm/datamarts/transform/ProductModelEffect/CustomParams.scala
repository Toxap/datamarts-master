package ru.beeline.cvm.datamarts.transform.ProductModelEffect

import ru.beeline.cvm.commons.CustomParamsInit
import ru.beeline.cvm.commons.hdfsutils.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit with utils {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val TableProductModelEffectSource: String = getAppConf(jobConfigKey, "tableProductModelEffectSource")
  val TableProductModelEffect: String = getAppConf(jobConfigKey, "tableProductModelEffect")

}
