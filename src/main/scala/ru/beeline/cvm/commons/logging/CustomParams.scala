package ru.beeline.cvm.commons.logging

import ru.beeline.cvm.commons.EntryPoint

class CustomParams extends EntryPoint{

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val pathInfoFile = getAppConf(jobConfigKey, "pathInfoFile")
  val loggingTableName = getAppConf(jobConfigKey, "loggingTableName")
  val loggingTablePath = getAppConf(jobConfigKey, "loggingTablePath")

}
