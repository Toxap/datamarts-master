package ru.beeline.cvm.datamarts.transform.callCycles

object Main extends callCycles {

  resultDf
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableCallCycles)
}
