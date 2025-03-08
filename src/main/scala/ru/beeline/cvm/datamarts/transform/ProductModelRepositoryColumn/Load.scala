package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumn

import org.apache.spark.sql.DataFrame
import ru.beeline.cvm.commons.hdfsutils.utils

class Load extends CustomParams with utils {

  def getSource(tableName: String): DataFrame = {
    spark.table(tableName)
      .drop("time_key")
  }

  def getproductChannelCost(tableName: String): DataFrame = {
    spark.table(tableName)
  }

  def getProductAll(tableName: String): DataFrame = {
    spark.table(tableName)
  }

  def getProductTreshhold(tableName: String): DataFrame = {
    spark.table(tableName)
  }

}
