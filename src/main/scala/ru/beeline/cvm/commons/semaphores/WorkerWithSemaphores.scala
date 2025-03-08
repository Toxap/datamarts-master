package ru.beeline.cvm.commons.semaphores

import ru.beeline.cvm.commons.metrics.WorkerVictoriaMetrics
import ru.beeline.dmp.clientvictoriametrics.commons.entity.Tag

case class WorkerWithSemaphores(workerVM: WorkerVictoriaMetrics) {

  def validSemaphores(lstTagForMetricStatus: List[(List[Tag], String)]): Boolean = {
    lstTagForMetricStatus.
      map(el => (workerVM.getLastMetricStatusApp(el._1), el._2)).
      collect {
      case (value, valuesForCondition)
        if value.isDefined && valuesForCondition.split(" ").map(_.toInt).contains(value.get.toInt) => false
    }.
      size match {
      case 0 => true
      case _ => false
    }
  }
}
