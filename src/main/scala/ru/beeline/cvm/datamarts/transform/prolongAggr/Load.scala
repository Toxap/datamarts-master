package ru.beeline.cvm.datamarts.transform.prolongAggr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class Load extends CustomParams {

  def getUniqueCtn: Long = {
    spark.table(TableProlongCustFeatures)
      .filter(col("time_key") === SCORE_DATE_1_days)
      .select(col("subs_key"))
      .distinct()
      .count()
  }

  def getUniqueRequestId: Long = {
    spark.table(TableProlongCustFeatures)
      .filter(col("time_key") === SCORE_DATE_1_days)
      .select(col("request_id"))
      .distinct()
      .count()
  }

  def getProlongOutput: DataFrame = {
    spark.table(TableProlongOutput)
      .filter(col("time_key") === SCORE_DATE_1_days)
  }

  def getProlongCustFeatures: DataFrame = {
    spark.table(TableProlongCustFeatures)
      .filter(col("time_key") === SCORE_DATE_1_days)
  }

}
