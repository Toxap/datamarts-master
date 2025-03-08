package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumn

object Main extends ProductModelRepository {

    productModelRepositoryColumn
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductModelRepositoryColumn)

}
