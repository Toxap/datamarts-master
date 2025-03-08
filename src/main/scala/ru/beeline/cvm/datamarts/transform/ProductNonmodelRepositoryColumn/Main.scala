package ru.beeline.cvm.datamarts.transform.ProductNonmodelRepositoryColumn

object Main extends ProductNonmodelRepository {

    productNonmodelRepository
      .coalesce(10)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductNonmodelRepositoryColumn)

}
