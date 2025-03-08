package ru.beeline.cvm.datamarts.transform.productClientBase

import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.functions._
object Main extends ProductClienBase {

  resultDf
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableProductClientBase)

}
