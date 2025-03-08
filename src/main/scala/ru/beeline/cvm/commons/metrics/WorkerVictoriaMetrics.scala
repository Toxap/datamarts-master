package ru.beeline.cvm.commons.metrics

import io.circe.syntax._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import ru.beeline.dmp.clientvictoriametrics.Client
import ru.beeline.dmp.clientvictoriametrics.commons.entity.{GetMetric, Metric, Query, Tag}

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}

case class WorkerVictoriaMetrics(conf: ConfigForWorkerVictoriaMetrics) extends Logging {

  log.info(s"Config for client Victoria Metrics:")
  log.info(s"URL - ${conf.urlVictoriaMetrics}")
  log.info(s"Port for open TSDB protocol - ${conf.openTsdbPort}")
  log.info(s"Path for open TSDB protocol - ${conf.openTsdbPath}")
  log.info(s"Port for prometheus protocol - ${conf.prometheusPort}")
  log.info(s"Path for prometheus protocol - ${conf.prometheusPath}")

  val clientVM = new Client(urlService = conf.urlVictoriaMetrics,
    openTsdbHttpPort = conf.openTsdbPort,
    openTsdbHttpPath = conf.openTsdbPath,
    prometheusPort = conf.prometheusPort,
    prometheusPath = conf.prometheusPath)

  lazy val relevanceTS = LocalDate.
    parse(conf.relevanceDate.value, DateTimeFormatter.ofPattern("yyyy-MM-dd")).
    atStartOfDay().atZone(ZoneId.of("Europe/Moscow")).
    toInstant().
    toEpochMilli()

  lazy val lstTagsForStatus = List(conf.product, conf.process, conf.source, conf.target)
  lazy val lstTagsForRelevance = List(conf.product, conf.target, conf.relevanceDate)
  lazy val lstTagsForStatistic: List[Tag] = List(conf.product, conf.target)

  def processingSendMetric(metrics: List[Metric], num: Int): Boolean = {

    val limitRetry = 20

    log.info(s"Value of metrics to send:\n${metrics.map(_.asJson).mkString("\n")}")

    val code = clientVM.sendMetric(metrics).code

    log.info(s"Attempt ${num} to send the metric")
    log.info(s"Response code: ${code}")

    if (limitRetry > num) {

      code match {
        case 204 => true
        case _ => {
          Thread.sleep(3 * 60 * 1000)
          processingSendMetric(metrics, num + 1)
        }
      }
    }
    else {throw new Exception(s"Failed to send metric, VM returned response code: ${code}") }

  }

  def sendMetricStatusApp(value: Int): Boolean = {
    val metricStatus = Metric(name = "spark.app.status", value = value, tags = lstTagsForStatus)
    if (value == 0) {
      Thread.sleep(60 * 1000)
    }
    processingSendMetric(List(metricStatus), 0)
  }

  def sendMetricRelevance(): Boolean = {
    val metricTimestampRelevanceData = Metric(name = "relevance.ts",
      value = relevanceTS,
      tags = lstTagsForRelevance)
    processingSendMetric(List(metricTimestampRelevanceData), 0)
  }

  def sendMetricContRecords(value: Long, tags: List[Tag] = lstTagsForStatistic): Boolean = {
    val metricCountRecords = Metric(name = "count.records", value = value, tags = tags)
    processingSendMetric(List(metricCountRecords), 0)
  }

  def sendMetricContRecords(value: Long, partName: String): Boolean = {
    val tagsWithPart: List[Tag] = lstTagsForStatistic :+ Tag("target_part", partName)
    sendMetricContRecords(value, tagsWithPart)
  }

  def sendStatMetrics(lst: List[Metric]): Boolean = {
    processingSendMetric(lst, 0)
  }

  def getLastMetricStatusApp(tags: List[Tag]): Option[Double] = {
    val query = Query(name = "spark.app.status", tags = tags)

    val lstGetMetric: Option[List[GetMetric]] = clientVM.getMetric(query)

    lstGetMetric match {
      case Some(lst) => {
        log.info(s"Value of metrics from VM:\n${
          lst.map(m => s"name: ${m.name}, " +
            s"tags: ${m.tags.map(t => s"${t.name}=${t.value}").mkString(",")}, " +
            s"values: ${m.values.map(el => s"${el._1}->${el._2}").mkString(",")}").mkString("\n")
        }")

        lst.size match {
          case 1 => {
            val getMetric = lst(0)
            val lastTs = getMetric.values.keySet.toList.sortWith(_.getTime > _.getTime).head
            val value = getMetric.values.get(lastTs)
            log.info(s"Last status for app: $value")
            value
          }
          case x if x > 1 => throw new Exception("Ambiguous request, more than one response returned")
          case _ => throw new Exception("Returned empty list")
        }
      }
      case None => None
    }
  }

  def isSameTaskRunning(): Boolean = {
    getLastMetricStatusApp(lstTagsForStatus) match {
      case Some(x) => if (x == 1.0) true else false
      case None => false
    }
  }

  def getListColumnStatistics(operationColumnsStrings: Map[String, String]): List[Column] = {
    count(col("*")).as("count.records") ::
      operationColumnsStrings.flatMap(operationColumnsString => operationColumnsString._2.split(",").
        map(_.trim).
        filter(_.nonEmpty).
        map(column => operationColumnsString._1 match {
          case "max" => max(col(column)).as("max.value." + column)
          case "min" => min(col(column)).as("min.value." + column)
          case "avg" => avg(col(column)).as("avg.value." + column)
          case "null" => count(when(col(column).isNull, 1)).as("count.null.records." + column)
        })).toList
  }

  def getListMetricStatisticsPartitioned(df: DataFrame, partitions: List[String],
                                         partitionField: String, statistic: List[Column]): List[Metric] = {
    log.info(s"getListMetricStatisticsPartitioned partitions: $partitions," +
      s" partitionField: $partitionField, statistic: $statistic")
    val a = partitions.
      flatMap(part =>
        getListMetricStatisticsCommonsFunc(df.filter(col(partitionField) === part), statistic, Some(part)))
    log.info(s"getListMetricStatisticsPartitioned done List[Metric] $a")
    a
  }


  def getListMetricStatisticsPartitionedParquet(spark: SparkSession, outputTablePath: String,
                                                partitions: List[String], partitionField: String,
                                                statistic: List[Column],
                                                tagLastPartitions: Option[Int] = None): List[Metric] =
    partitions.zipWithIndex.flatMap { x =>
      val (part, i) = x
      val customTags = if (tagLastPartitions.isDefined) {
        List(Tag("flag_last_part", if (i > partitions.size - 1 - tagLastPartitions.get) "1" else "0"))
      } else { Nil }
      val path = outputTablePath + "/" + partitionField + "=" + part
      try {
        getListMetricStatisticsCommonsFunc(spark.read.parquet(path), statistic, Some(part), customTags)
      }
      catch {
        case e: org.apache.spark.sql.AnalysisException => Nil
        case e: Throwable => throw e
      }
    }

  def getListMetricStatistics(df: DataFrame, statistic: List[Column]): List[Metric] =
    getListMetricStatisticsCommonsFunc(df, statistic, None)

  def getListMetricStatisticsCommonsFunc(df: DataFrame, statistic: List[Column],
                                         part: Option[String], customTags: List[Tag] = Nil): List[Metric] = {

    if (!df.take(1).isEmpty) {
      log.info(s"!df.take(1).isEmpty")
      val dfAgg = df.agg(statistic.head, statistic.tail: _*)
      dfAgg.columns.zip(dfAgg.head().toSeq.map(_.toString.toDouble))
        .map { x =>
          val (nameMetric, nameField) = getNameMetricAndField(x._1)
          val updTags: List[Tag] = lstTagsForStatistic :::
            (if (part.isDefined) List(Tag("target_part", part.get)) else Nil) :::
            (if (nameField.isDefined) List(Tag("target_field", nameField.get)) else Nil) :::
            customTags

          Metric(name = nameMetric, value = x._2, tags = updTags)
        }.toList
    }
    else {
      log.info(s"df.take(1).isEmpty Nil")
      Nil
    }
  }

  def getNameMetricAndField(nameField: String): (String, Option[String]) = {
    if (nameField != "count.records") {
      (nameField.split("\\.").reverse.tail.reverse.mkString("."),
        Some(nameField.split("\\.").reverse.head))
    }
    else {
      (nameField, None)
    }
  }

}
