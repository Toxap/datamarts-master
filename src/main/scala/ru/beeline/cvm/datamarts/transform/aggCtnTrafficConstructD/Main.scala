package ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD

import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics
import ru.beeline.cvm.commons.validation.ValidateExecutionCode
import ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD.staging.AggCtnTrafficConstructD

import scala.util.Try

object Main extends AggCtnTrafficConstructD {

  val workerVM = WorkerVictoriaMetrics(confForWorkerVictoriaMetrics)

  val execution = Try {

    if (workerVM.isSameTaskRunning()) {
      throw new Exception("The same App is already running or not completed correctly")
    }

    workerVM.sendMetricStatusApp(value = 1)

    persistSources
    res
      .repartition(NUM_PARTITIONS)
      .write
      .format("parquet")
      .mode("overwrite")
      .insertInto(TableAggCtnTrafficConstructD)
    unpersistSources

    val resultDf = spark.read.table(TableAggCtnTrafficConstructD)
    resultDf.persist()

    val listColumnStatistics = workerVM.getListColumnStatistics(statisticFields)
    val metrics = workerVM.getListMetricStatistics(df = resultDf, statistic = listColumnStatistics)
    workerVM.sendStatMetrics(metrics)

    resultDf.unpersist()
    spark.stop()
  }
  ValidateExecutionCode.matchResult(execution, workerVM)

}
