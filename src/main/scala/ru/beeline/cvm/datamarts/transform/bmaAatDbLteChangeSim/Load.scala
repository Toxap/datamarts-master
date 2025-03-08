package ru.beeline.cvm.datamarts.transform.bmaAatDbLteChangeSim

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, concat, current_date, date_add, lit, max, min, when}
class Load extends CustomParams {

  def tCamps: DataFrame = {
    spark.table(TableTCamps)
  }

  def cmsMemberss: DataFrame = {
    spark.table(TableCmsMembers)
  }

  def aggTimeSpendPositionn: DataFrame = {
    spark.table(TableAggTimeSpendPositionCodesPubD)
  }

  def dimUcnNeww: DataFrame = {
    spark.read
      .option("delimiter", ";")
      .option("header","true")
      .option("encoding","windows-1251")
      .csv("/apps/airflow/tech_cvmbox_bgd_ms/datamarts/sources/bma_aat_dim_ucn_new_202407081510.csv")

  }

  def lteAfterr: DataFrame = {
    spark.table(TableStgSimLteDaily)
}

  def devicee: DataFrame = {
    spark.table(TableStgDeviceChanged)
  }

  def dataTrafic: DataFrame = {
    spark.table(TableUsage)
  }
  def loginRefarmm: DataFrame = {
    spark.table(TableBmaAatLoginRefarm)
  }
}
