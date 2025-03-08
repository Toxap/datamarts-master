package ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct

import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics
import ru.beeline.cvm.commons.validation.ValidateExecutionCode
import ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct.staging.UpsellPresetConstruct

import scala.util.Try

object Main extends UpsellPresetConstruct {

  val workerVM = WorkerVictoriaMetrics(confForWorkerVictoriaMetrics)

  val execution = Try {
    workerVM.sendMetricStatusApp(value = 1)

    res.repartition(1)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableDimUpsellPresetConstruct)


    val resultDf = spark.read.table(TableDimUpsellPresetConstruct)
    resultDf.persist()

    val listColumnStatistics = workerVM.getListColumnStatistics(statisticFields)
    val metrics = workerVM.getListMetricStatistics(df = resultDf, statistic = listColumnStatistics)
    workerVM.sendStatMetrics(metrics)

    resultDf.unpersist()
    spark.catalog.clearCache()
    spark.stop()
  }
  ValidateExecutionCode.matchResult(execution, workerVM)
}
