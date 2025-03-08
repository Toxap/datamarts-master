package ru.beeline.cvm.datamarts.transform.fctCampEffects

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Load extends CustomParams {

  private val parentFilter = col("parent_isn") === 42641 && col("parent_isn") =!= col("isn")

  def getCampLibrary: DataFrame = {

    spark.table(TableCampLibrary)
      .repartition(80, col("camp_id"))
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK);

  }

  def getWave: DataFrame = {

    broadcast(spark.table(TableWave))

  }

  def getCampToEventType: DataFrame = {

    spark.table(TableCampToEventType)
      .repartition(50, col("camp_id"))
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK);

  }

  def getCumulativeEffCampaing: DataFrame = {

    spark.table(TableCumulativeEffCampaing)
      .repartition(80, col("camp_id"))
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK);

  }

  def getTCampsLastPart: DataFrame = {

    spark.table(TableTCampsLastPart)
      .repartition(50, col("camp_id"))
      .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK);

  }

  def getDimDic: DataFrame = {

    broadcast(spark.table(TableDimDic).filter(parentFilter))

  }

  def getFctAvgSaleIsnUseCase: DataFrame = {

    broadcast(spark.table(TableFctAvgSaleIsnUseCase))

  }

}
