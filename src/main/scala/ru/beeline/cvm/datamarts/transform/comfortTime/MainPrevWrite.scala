package ru.beeline.cvm.datamarts.transform.comfortTime

import org.apache.spark.sql.functions._

object MainPrevWrite extends ComfortTime {


  val rowCount = nbaCtnTz.count()
  if (rowCount >= 50000000) {

  nbaCtnTz
    .select(
      col("subs_key").as("ctn"),
      col("tz"),
      col("weeks_ago")
    )
    .repartition(1)
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.nba_ctn_tz_prev_temp")
  } else {
    spark.stop()
  }

}
