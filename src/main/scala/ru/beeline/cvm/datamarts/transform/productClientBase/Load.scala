package ru.beeline.cvm.datamarts.transform.productClientBase

import org.apache.spark.sql.DataFrame
class Load extends CustomParams {

  def aggSubsProfile: DataFrame = {
    spark.table(TableAggSubsProfilePubD)
  }

}
