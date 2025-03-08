package ru.beeline.cvm.commons

import ru.beeline.cvm.commons.metrics.ConfigForWorkerVictoriaMetrics
import ru.beeline.dmp.clientvictoriametrics.commons.entity.Tag

class CustomParamsInit extends EntryPoint {

  val jobConfigKey = "spark." + spark.sparkContext.appName + "."

  val tomorrowDs = spark.conf.get(jobConfigKey + "tomorrowDs")

  val urlVictoriaMetrics = getAppConf(jobConfigKey, "urlVictoriaMetrics")
  val openTsdbPort = getAppConf(jobConfigKey, "openTsdbPort").toInt
  val prometheusPort = getAppConf(jobConfigKey, "prometheusPort").toInt
  val openTsdbPath = getAppConf(jobConfigKey, "openTsdbPath")
  val prometheusPath = getAppConf(jobConfigKey, "prometheusPath")

  val loggingTableName = getAppConf(jobConfigKey, "loggingTableName")
  val loggingTablePath = getAppConf(jobConfigKey, "loggingTablePath")

  private val countBlocking = getAppConf(jobConfigKey, "blocking.count").toInt
  val lstTagForMetricStatus: List[(List[Tag], String)] = countBlocking match {
    case 0 => Nil
    case x if x > 0 => {
      (1 to countBlocking).map { number =>
        val tagProduct = Tag(name = "product",
          value = getAppConf(jobConfigKey, s"blocking.process_${number}.product"))
        val tagProcess = Tag(name = "process",
          value = getAppConf(jobConfigKey, s"blocking.process_${number}.process"))
        val tagSrc = Tag(name = "source",
          value = getAppConf(jobConfigKey, s"blocking.process_${number}.source"))
        val tagTrg = Tag(name = "target",
          value = getAppConf(jobConfigKey, s"blocking.process_${number}.target"))
        val valueForCondition = getAppConf(jobConfigKey, s"blocking.process_${number}.value.condition")
        (List(tagProduct, tagProcess, tagSrc, tagTrg), valueForCondition)
      }.toList
    }
    case _ => throw new IllegalArgumentException("Value for argument blocking.count does not match valid")
  }

  val confForWorkerVictoriaMetrics = ConfigForWorkerVictoriaMetrics(
    urlVictoriaMetrics = urlVictoriaMetrics,
    openTsdbPort = openTsdbPort,
    openTsdbPath = openTsdbPath,
    prometheusPort = prometheusPort,
    prometheusPath = prometheusPath,
    product = Tag("product", getAppConf(jobConfigKey, "product")),
    process = Tag("process", getAppConf(jobConfigKey, "process")),
    source = Tag("source", getAppConf(jobConfigKey, "source")),
    target = Tag("target", getAppConf(jobConfigKey, "target")),
    relevanceDate = Tag("relevance.date", tomorrowDs))

  lazy val maxStatistic = Map("max" -> getAppConf(jobConfigKey, "statistics.max"))
  lazy val minStatistic = Map("min" -> getAppConf(jobConfigKey, "statistics.min"))
  lazy val avgStatistic = Map("avg" -> getAppConf(jobConfigKey, "statistics.avg"))
  lazy val nullStatistic = Map("null" -> getAppConf(jobConfigKey, "statistics.null"))
  lazy val statisticFields = maxStatistic ++ minStatistic ++ avgStatistic ++ nullStatistic

}
