package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.DataFrame
class Load extends CustomParams {

  def callOutBound: DataFrame = {
    spark.table(TableIntrxnPub)
  }

  def getBan: DataFrame = {
    spark.table(TableFinAccntPub)
  }

  def reason1: DataFrame = {
    spark.table(TableTopicPub)
  }

  def cmDatamart: DataFrame = {
    spark.table(TableAggCtnChurnPaidPubM)
  }

  def getDimCtnBan: DataFrame = {
    spark.table(TableDimCtnBan)
  }

  def outbCrmBase1: DataFrame = {
    spark.table(TableOutbCrmBase202101V2)
  }

  def atsSmCsBase2: DataFrame = {
    spark.table(TableSmCsBase2)
  }


  def atsSmCsBase22: DataFrame = {
    spark.table(TableSmCsBase22)
  }

  def atsSmCsCycles2: DataFrame = {
    spark.table(TableSmCsCycles2)
  }

  def atsSmCsCyclesPrev2: DataFrame = {
    spark.table(TableCallCyclesPrev2)
  }

  def smCsCyclesNext1: DataFrame = {
    spark.table(TableSmCsCyclesNext)
  }

  def atsSmCsCyclesFull: DataFrame = {
    spark.table(TableSmCsCyclesFull)
  }

  def atsSmCsCyclesDataV2: DataFrame = {
    spark.table(TableSmCsCyclesDataV2)
  }


  def atsSmCsCyclesData2: DataFrame = {
    spark.table(TableSmCsCyclesData2)
  }

  def atsSmCsCyclesPrevsV2: DataFrame = {
    spark.table(TableCallCyclesPrevsV2)
  }

  def tableDmMobileAttr: DataFrame = {
    spark.table(TableDmMobileSubscriberAttr)
  }

  def baseB2c22Table: DataFrame = {
    spark.table(TableDmMobileSubscriber)
  }

  def baseB2c23Table: DataFrame = {
    spark.table(TableDmMobileSubscriberMonthly)
  }

}
