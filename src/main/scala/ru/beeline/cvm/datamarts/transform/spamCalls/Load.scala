package ru.beeline.cvm.datamarts.transform.spamCalls

import org.apache.spark.sql.DataFrame

class Load extends CustomParams {

  def tableDmMobileAttr: DataFrame = {
    spark.table(TableDmMobileSubscriberAttr)
  }

  def baseB2c22Table: DataFrame = {
    spark.table(TableDmMobileSubscriber)
  }

  def baseB2c23Table: DataFrame = {
    spark.table(TableDmMobileSubscriberMonthly)
  }

  def incomeCallsTableData: DataFrame = {
    spark.table(TableBiisFixTrafUnion)
  }

  def spamBaseTableData: DataFrame = {
    spark.table(TableAntispamBinaryBaseScoring)
  }
}
