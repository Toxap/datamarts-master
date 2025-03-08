package ru.beeline.cvm.datamarts.transform.bmaAatDbLteChangeSim

object Main extends BmaAatDbLteChangeSim {

  step1
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableBmaAatDbLteChangeSim)

}
