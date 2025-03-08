package ru.beeline.cvm.datamarts.transform.prolongModelCorrected

import org.apache.spark.sql.functions.col

object ProlongOutput extends CustomParams {

  val prolongOutput = spark.table(TableProlongOutput)
    .filter(col("time_key") === SCORE_DATE)

  prolongOutput
    .coalesce(numPartiton)
    .write
    .partitionBy("time_key")
    .mode("overwrite")
    .format("orc")
    .saveAsTable(TableProlongOutputCorrected)
}
