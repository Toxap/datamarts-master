package ru.beeline.cvm.commons.filemerger

import io.circe.jawn

object FileMerger extends CustomParams {

  log.info(s"Start application ${spark.sparkContext.appName}")

  val arrStrJson = spark.read.textFile(pathInfoFile).collect.toList
  val workerFileMerger = WorkerFileMerger(spark)

  arrStrJson.foreach {
    strJson =>
      log.info(s"Json from file: ${strJson}")

      val infoPath = jawn.decode[InfoPathTable](strJson) match {
        case Right(infoPath) => infoPath
        case Left(_) => throw new Exception(s"Failed parsing JSON: $strJson")
      }
      workerFileMerger.merger(path = infoPath.path,
        exclusion = infoPath.exclusion,
        repartition = infoPath.repartition)
  }

  spark.stop()

  log.info(s"Application ended with success")
}
