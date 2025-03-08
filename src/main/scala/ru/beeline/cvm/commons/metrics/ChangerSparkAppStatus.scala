package ru.beeline.cvm.commons.metrics

import io.circe.jawn
import ru.beeline.dmp.clientvictoriametrics.Client
import ru.beeline.dmp.clientvictoriametrics.commons.entity.{Metric, Tag}

object ChangerSparkAppStatus extends CustomParams {

  log.info(s"Start application ${spark.sparkContext.appName}")

  log.info(s"Config for client Victoria Metrics:")
  log.info(s"URL - ${urlVictoriaMetrics}")
  log.info(s"Port for open TSDB protocol - ${openTsdbPort}")
  log.info(s"Path for open TSDB protocol - ${openTsdbPath}")
  log.info(s"Port for prometheus protocol - ${prometheusPort}")
  log.info(s"Path for prometheus protocol - ${prometheusPath}")

  val clientVM = new Client(urlService = urlVictoriaMetrics,
    openTsdbHttpPort = openTsdbPort,
    openTsdbHttpPath = openTsdbPath,
    prometheusPort = prometheusPort,
    prometheusPath = prometheusPath)

  val arrStrJson = spark.read.textFile(pathInfoFile).collect.toList

  val metrics = arrStrJson.map { strJson =>

    log.info(s"Json from file: ${strJson}")

    val updAppStatus = jawn.decode[AppStatus](strJson) match {
      case Right(appStatus) => appStatus
      case Left(_) => throw new Exception(s"Failed parsing JSON: $strJson")
    }

    val tagProduct = Tag(name = "product", updAppStatus.product)
    val tagProcess = Tag(name = "process", updAppStatus.process)
    val tagSrc = Tag(name = "source", updAppStatus.source)
    val tagTrg = Tag(name = "target", updAppStatus.target)

    val lstTagsForStatus = List(tagProduct, tagProcess, tagSrc, tagTrg)

    Metric(name = "spark.app.status", value = updAppStatus.value, tags = lstTagsForStatus)

  }

  clientVM.sendMetric(metrics)
  log.info(s"Application ended with success")
}
