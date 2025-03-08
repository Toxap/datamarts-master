package ru.beeline.cvm.datamarts.transform.prolongAggr

object Main extends prolongAggr {

  prolongAggrFinalTable
    .repartition(4)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableProlongAggr)

}
