package ru.beeline.cvm.datamarts.transform.ProductChannelCost

object Main extends ProductTreshholdModel {

    res
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductChannelCost)

}
