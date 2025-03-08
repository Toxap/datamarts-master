package ru.beeline.cvm.datamarts.transform.coreModelBorders

import ru.beeline.cvm.commons.CustomParamsInit

class CustomParams extends CustomParamsInit {

  val TableCoreModelScoring = getAppConf(jobConfigKey, "tableCoreModelScoring")
  val TableCoreBordersDaily = getAppConf(jobConfigKey, "tableCoreBordersDaily")
  val TableCoreBordersMonthly = getAppConf(jobConfigKey, "tableCoreBordersMonthly")
  val TableCoreStatisticsDaily = getAppConf(jobConfigKey, "tableStatisticsDaily")
  val TableCoreStatisticsMonthly = getAppConf(jobConfigKey, "tableStatisticsMonthly")

}
