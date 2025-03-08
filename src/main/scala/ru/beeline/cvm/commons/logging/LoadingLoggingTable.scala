package ru.beeline.cvm.commons.logging

import io.circe.jawn
import ru.beeline.cvm.commons.metrics.WorkerWithMetrics

@Deprecated
object LoadingLoggingTable extends CustomParams {

 val arrStrJson = spark.read.textFile(pathInfoFile).collect.toList

  val workerWithMetrics = new WorkerWithMetrics(spark)
  val workerLoggingTable = WorkerLoggingTable(spark = spark,
    techTableName = loggingTableName, techTablePath = loggingTablePath)

  arrStrJson.foreach { strJson =>

    val infoCsv = jawn.decode[InfoCsvFile](strJson) match {
      case Right(infoCsvFile) => infoCsvFile
      case Left(_) => throw new Exception(s"Failed parsing JSON: $strJson")
    }

    workerLoggingTable.writeMetricToTable(values = workerWithMetrics.readMetricsFromCsvFormat(infoCsv.path),
      tableTrgName = infoCsv.tableTrgName, tableSrcName = infoCsv.tableSrcName, nameMetric = infoCsv.nameMetric)
  }
}
