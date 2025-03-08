package ru.beeline.cvm.datamarts.transform.productCategoryProfile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Load extends CustomParams {

  def tablModelScore: DataFrame = {
    spark.table(TableModelScore)
  }

  def thresholdProduct: DataFrame = {
    spark.table(TableProductTreshholdModelSource)
  }

  def tableAanaaMvp: DataFrame = {
    spark.table(TableAanaaMvpCategorization)
  }

  def tableAnaMvp: DataFrame = {
    spark.table(TableAnaMvpCategorization)
  }



}
