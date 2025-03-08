package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumnNonBlock

object Main extends ProductModelRepositoryNonBlock {

    updatedProductModelRepository
      .drop("has_no_block")
      .drop("block_reasons_array")
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableProductModelRepositoryColumnNonBlock)

}
