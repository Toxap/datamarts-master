package ru.beeline.cvm.datamarts.transform.ProductTreshholdModel

object Main extends ProductTreshholdModel {

    res
      .coalesce(2)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductTreshhold)

}
