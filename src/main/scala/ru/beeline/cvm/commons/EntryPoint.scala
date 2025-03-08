package ru.beeline.cvm.commons

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class EntryPoint extends App with LightBendConf with Logging{

  implicit val spark: SparkSession = SparkSession.builder
    .master("yarn")
    .config("hive.exec.dynamic.partition", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    .enableHiveSupport()
    .getOrCreate()

  val confName = spark.conf.get(s"spark.${spark.sparkContext.appName}.confName", "")
  val jobConfig = appProps(spark.sparkContext.appName)

  def getAppConf(key: String, field: String): String = {
    spark.conf.get(key + field, jobConfig.getString(field))
  }
}
