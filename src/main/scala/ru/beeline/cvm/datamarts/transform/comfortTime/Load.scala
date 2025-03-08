package ru.beeline.cvm.datamarts.transform.comfortTime

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, concat, current_date, date_add, lit, max, min, when}

class Load extends CustomParams {

  def getLastPartFromAggSubs: String = {
    spark.table(TableAggSubsProfile)
      .select(max("calendar_dt"))
      .distinct()
      .collect()(0)(0)
      .toString
  }

  def getStgGeoAll: DataFrame = {
    spark.table(TableStgGeoAll)
      .filter(col("time_key") >= date_add(current_date(), -7))
  }
  def getAggSubsProfile(part: String): DataFrame = {
    spark.table(TableAggSubsProfile)
      .filter(col("subscriber_status_cd").isin("A", "S"))
      .filter(col("calendar_dt") === part)
      .orderBy(col("init_activation_dt").desc)
      .select(
        col("subscriber_num").as("subs_key"),
        col("market_cd").as("market_key")
      )
      .dropDuplicates("subs_key")
  }
  def getTrefMarketInfo: DataFrame = {
    spark.table(TableTrefMarketInfo)
  }

  def getNbaCtnTzPrev: DataFrame = {
    spark.table(TableNbaCtnTzPrev)
      .groupBy("ctn")
      .agg(min(col("tz")).as("tz"), (min(col("weeks_ago")) + 1).as("weeks_ago"))
      .select(
        col("ctn"),
        col("tz"),
        col("weeks_ago")
      )
  }




}
