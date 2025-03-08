package ru.beeline.cvm.commons.hiveutils

import ru.beeline.cvm.commons.EntryPoint

class CustomParams extends EntryPoint{

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val hdfsPathSource = spark.conf.getOption(jobConfigKey + "hdfsPathSource")
  val hdfsPathTarget = spark.conf.getOption(jobConfigKey + "hdfsPathTarget")
  val tableName = spark.conf.getOption(jobConfigKey + "tableName")
  val firstPart = spark.conf.getOption(jobConfigKey + "firstPart")
  val lastPart = spark.conf.getOption(jobConfigKey + "lastPart")

}
