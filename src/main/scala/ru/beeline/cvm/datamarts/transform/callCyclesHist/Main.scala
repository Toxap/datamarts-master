package ru.beeline.cvm.datamarts.transform.callCyclesHist

object Main extends callCyclesHist {
  getResultDF
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableCallCyclesHist)
}

