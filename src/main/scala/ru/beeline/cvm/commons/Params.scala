package ru.beeline.cvm.commons

import com.typesafe.config.Config
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import Params.log

import scala.util.{Failure, Success, Try}

class Params(implicit spark: SparkSession) extends LightBendConf with Serializable {

  private val appName = getSparkConfig("spark.app.name", prefix = "")
  private val jobConfig: Config = appProps(appName)
  log.info(s"jobConfig is: $jobConfig")

  def getValue(conf: String): String = {
    Try(getSparkConfig(conf)) match {
      case Success(value) => value
      case Failure(_) =>
        val confVal = jobConfig.getString(conf)
        log.warn(s"Load from application.conf: ${conf} -> ${confVal}")
        confVal
    }
  }

  private def getSparkConfig(conf: String, prefix: String = s"spark.${appName}."): String = {
    Try(spark.conf.get(prefix + conf)) match {
      case Success(value) =>
        log.warn(s"Load from spark.conf: $conf -> $value")
        value
      case Failure(exception) =>
        log.error(s"spark.conf not found: ${exception.getMessage}")
        throw exception
    }
  }


}

object Params {
  private val log: Logger = Logger.getLogger(getClass)
}
