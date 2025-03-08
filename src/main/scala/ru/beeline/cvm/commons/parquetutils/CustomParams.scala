package ru.beeline.cvm.commons.parquetutils

import ru.beeline.cvm.commons.EntryPoint

class CustomParams extends EntryPoint{

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val pathInfoFile = getAppConf(jobConfigKey, "pathInfoFile")
  val coalesce = getAppConf(jobConfigKey,"coalesce").toInt
}
