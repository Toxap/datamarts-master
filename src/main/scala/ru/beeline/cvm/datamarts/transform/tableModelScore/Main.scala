package ru.beeline.cvm.datamarts.transform.tableModelScore

object Main extends TableModelScore {

  tableModelScore
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableModelScore)


}
