package ru.beeline.cvm.commons.filemerger

import ru.beeline.cvm.commons.EntryPoint

class CustomParams extends EntryPoint {

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val pathInfoFile = getAppConf(jobConfigKey, "pathInfoFile")

}

