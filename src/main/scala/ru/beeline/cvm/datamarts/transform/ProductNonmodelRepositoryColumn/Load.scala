package ru.beeline.cvm.datamarts.transform.ProductNonmodelRepositoryColumn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import ru.beeline.cvm.commons.hdfsutils.utils

class Load extends CustomParams with utils {

  def getSource(tableName: String): DataFrame = {
    spark.table(tableName)
      .drop("time_key")
  }

  def getproductModelEffect(tableName: String): DataFrame = {
    spark.table(tableName)
      .drop("report_dt")
  }

  def getproductChannelCost(tableName: String): DataFrame = {
    spark.table(tableName)
  }

}
