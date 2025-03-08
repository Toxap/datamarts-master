package ru.beeline.cvm.datamarts.transform.prolongModelCorrected

import org.apache.spark.sql.functions.col

object ProlongCustFeatures extends CustomParams {

  val prolongCustFeatures = spark.table(TableProlongCustFeatures)
    .filter(col("time_key") === SCORE_DATE)

  prolongCustFeatures
    .coalesce(numPartiton)
    .write
    .partitionBy("time_key")
    .mode("append")
    .format("orc")
    .saveAsTable(TableProlongCustFeaturesCorrected)

}
