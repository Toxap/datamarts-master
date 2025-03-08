package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import org.apache.spark.sql.functions.col
import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics
import ru.beeline.cvm.commons.validation.ValidateExecutionCode

import scala.util.Try

object Main extends AntiDownSellDataset {

  val workerVM = WorkerVictoriaMetrics(confForWorkerVictoriaMetrics)

  val execution = Try {

    if (workerVM.isSameTaskRunning()) {
      throw new Exception("The same App is already running or not completed correctly")
    }

    workerVM.sendMetricStatusApp(value = 1)

    res.repartition(15)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableAdsDataset)

    val resultDf = spark.table(TableAdsDataset)
      .filter(col("time_key")===SCORE_DATE)
    resultDf.persist()

    val listColumnStatistics = workerVM.getListColumnStatistics(statisticFields)
    val metrics = workerVM
      .getListMetricStatisticsPartitioned(
        df = resultDf,
        partitions = List(SCORE_DATE),
        partitionField = partitionField,
        statistic = listColumnStatistics
      )
    workerVM.sendStatMetrics(metrics)

    resultDf.unpersist()
    spark.stop()
  }
  ValidateExecutionCode.matchResult(execution, workerVM)

}
