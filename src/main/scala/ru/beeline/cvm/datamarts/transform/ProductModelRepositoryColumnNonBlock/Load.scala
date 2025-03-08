package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumnNonBlock

import org.apache.spark.sql.DataFrame
import ru.beeline.cvm.commons.hdfsutils.utils

class Load extends CustomParams with utils {

  def getProductModelRepository(tableName: String): DataFrame = {
    spark.table(tableName)
  }

}
