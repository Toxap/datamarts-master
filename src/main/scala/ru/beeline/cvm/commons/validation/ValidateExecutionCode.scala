package ru.beeline.cvm.commons.validation

import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics

import scala.util.{Failure, Success, Try}

object ValidateExecutionCode {

  def matchResult(resultExecution: Try[Unit], workerVM: WorkerVictoriaMetrics, sendRelevance: Boolean = true): Boolean = {
    resultExecution match {
      case Success(_) => {
        if (sendRelevance) workerVM.sendMetricRelevance()
        workerVM.sendMetricStatusApp(value = 0)
      }
      case Failure(exception) => {
        workerVM.sendMetricStatusApp(value = -1)
        throw exception
      }
    }
  }


}
