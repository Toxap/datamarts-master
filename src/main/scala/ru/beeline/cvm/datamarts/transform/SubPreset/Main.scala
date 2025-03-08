package ru.beeline.cvm.datamarts.transform.SubPreset

object Main extends SubPreset {

  res
    .repartition(3)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableSubPreset)

}
