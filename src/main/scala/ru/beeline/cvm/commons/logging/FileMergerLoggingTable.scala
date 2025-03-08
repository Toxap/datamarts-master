package ru.beeline.cvm.commons.logging

import ru.beeline.cvm.commons.CustomParamsInit
import ru.beeline.cvm.commons.filemerger.WorkerFileMerger
import ru.beeline.cvm.commons.metrics.ChangerSparkAppStatus.log
import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics
import ru.beeline.cvm.commons.semaphores.WorkerWithSemaphores
import ru.beeline.cvm.commons.validation.ValidateExecutionCode

import scala.util.Try

object FileMergerLoggingTable extends CustomParamsInit {

  log.info(s"Start application ${spark.sparkContext.appName}")

  val workerVM = WorkerVictoriaMetrics(confForWorkerVictoriaMetrics)
  val workerBlocking = WorkerWithSemaphores(workerVM)
  val workerFileMerger = WorkerFileMerger(spark)

  while (!workerBlocking.validSemaphores(lstTagForMetricStatus)) {
    Thread.sleep(5 * 60 * 1000)
  }

  val execution = Try {

    if (workerVM.isSameTaskRunning()) {
      throw new Exception("The same App is already running or not completed correctly")
    }

    workerVM.sendMetricStatusApp(value = 1)

    workerFileMerger.merger(loggingTablePath, 0, 1)

    spark.stop()
  }

  ValidateExecutionCode.matchResult(execution, workerVM, false)
  log.info(s"Application ended with success")
}
