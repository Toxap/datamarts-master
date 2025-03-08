package ru.beeline.cvm.commons.metrics

import ru.beeline.cvm.commons.EntryPoint

class CustomParams extends EntryPoint{

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val pathInfoFile = getAppConf(jobConfigKey, "pathInfoFile")
  val urlVictoriaMetrics = getAppConf(jobConfigKey, "urlVictoriaMetrics")
  val openTsdbPort = getAppConf(jobConfigKey, "openTsdbPort").toInt
  val prometheusPort = getAppConf(jobConfigKey, "prometheusPort").toInt
  val openTsdbPath = getAppConf(jobConfigKey, "openTsdbPath")
  val prometheusPath = getAppConf(jobConfigKey, "prometheusPath")
}
