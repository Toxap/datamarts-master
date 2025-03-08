package ru.beeline.cvm.datamarts.transform.ProductModelEffect

object Main extends ProductModelEffect {

    res
      .coalesce(1)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductModelEffect)

}
