package ru.beeline.cvm.datamarts.transform.subPresetRoaming

object Main extends subPresetRoaming {

  dfMerged
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableSubPresetRoaming)
}
