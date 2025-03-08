package ru.beeline.cvm.datamarts.transform.productCategoryProfile

import org.apache.spark.sql.functions._

object Main extends ProductCategoryProfile {

  superFinalDf
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableProductCategoryProfile)


}
