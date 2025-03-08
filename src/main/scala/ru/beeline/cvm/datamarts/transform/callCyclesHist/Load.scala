package ru.beeline.cvm.datamarts.transform.callCyclesHist

import org.apache.spark.sql.DataFrame


class Load extends CustomParams {
  def getDimCtnBan: DataFrame = {
    spark.table(TableDimCtnBan)
  }

  def getCallCycles: DataFrame = {
    spark.table(TableCallCycles)
  }

}
