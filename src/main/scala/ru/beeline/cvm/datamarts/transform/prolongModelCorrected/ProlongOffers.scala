package ru.beeline.cvm.datamarts.transform.prolongModelCorrected

import org.apache.spark.sql.functions.col

object ProlongOffers extends CustomParams {

  private val prolongOffers = spark.table(TableProlongOffersAvailable)
    .filter(col("time_key") === SCORE_DATE)

  prolongOffers
    .coalesce(numPartiton)
    .write
    .partitionBy("time_key")
    .mode("overwrite")
    .format("orc")
    .saveAsTable(TableProlongOffersAvailableCorrected)

}
