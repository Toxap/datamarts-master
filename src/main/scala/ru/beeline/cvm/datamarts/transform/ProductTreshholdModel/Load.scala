package ru.beeline.cvm.datamarts.transform.ProductTreshholdModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class Load extends CustomParams {

  def getProductTreshholdModelSource(tableName: String): DataFrame = {

    val maxTimeKey = getMaxTimeKey(tableName)

    spark.table(tableName)
      .filter(col("report_dt") === maxTimeKey)

  }

}
