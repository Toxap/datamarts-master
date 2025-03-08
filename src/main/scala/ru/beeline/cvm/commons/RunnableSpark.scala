package ru.beeline.cvm.commons

import org.apache.spark.sql.SparkSession

class RunnableSpark extends App {

  private val conf = new MainConf(args)
  val appName = conf.appName()
  val packageName = conf.packageName()
  val classNameFamilyTarget = conf.FamilyTarget()

  implicit val spark: SparkSession = SparkSession.builder
    .appName(appName)
    .enableHiveSupport()
    .getOrCreate()

}
