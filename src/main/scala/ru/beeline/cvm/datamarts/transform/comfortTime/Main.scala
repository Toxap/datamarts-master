package ru.beeline.cvm.datamarts.transform.comfortTime

import org.apache.spark.sql.functions._

object Main extends ComfortTime {

  val rowCount = nbaCtnTz.count()
  if (rowCount >= 50000000) {

    val resultDf = nbaCtnTz
      .withColumn("startComfortTime", concat(lit("09:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
        .otherwise(col("tz"))))
      .withColumn("endComfortTime", concat(lit("22:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
        .otherwise(col("tz"))))
      .withColumn("timezoneMethod", when(col("used_market_tz")===0, lit("GEOZONE")).otherwise(lit("MARKET_CODE")))
      .select(
        col("subs_key").as("ctn"),
        col("startComfortTime"),
        col("endComfortTime"),
        col("timezoneMethod")
      );

    val resultDfOnlyChanges = nbaCtnTzOnlyChanges
      .withColumn("startComfortTime", concat(lit("09:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
        .otherwise(col("tz"))))
      .withColumn("endComfortTime", concat(lit("22:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
        .otherwise(col("tz"))))
      .withColumn("timezoneMethod", when(col("used_market_tz")===0, lit("GEOZONE")).otherwise(lit("MARKET_CODE")))
      .select(
        col("subs_key").as("ctn"),
        col("startComfortTime"),
        col("endComfortTime"),
        col("timezoneMethod")
      );

    val testctn = spark.read.option("header", true).csv("/apps/airflow/tech_cvmbox_bgd_ms/datamarts/sources/testctn.csv")
      .withColumn("startcomforttime", lit("00:00:00+03"))
      .withColumn("endcomforttime", lit("23:59:59+03"))
      .withColumn("timezonemethod", lit("MARKET_CODE"))

    val mainAlias = resultDfOnlyChanges.alias("main");
    val testAlias = testctn.alias("test");

    val existingRecords = mainAlias.join(testAlias, Seq("ctn"), "inner")
      .select(
        col("main.ctn"),
        col("test.startcomforttime").alias("startcomforttime"),
        col("test.endcomforttime").alias("endcomforttime"),
        col("test.timezonemethod").alias("timezonemethod")
      )

    val newRecords = testAlias.join(mainAlias, Seq("ctn"), "left_anti")

    val existingRecordsAlias = existingRecords.alias("existing")

    val updatedMainDF = mainAlias.join(existingRecordsAlias, Seq("ctn"), "left_outer")
      .withColumn("startcomforttime_upd", coalesce(col("existing.startcomforttime"), col("main.startcomforttime")))
      .withColumn("endcomforttime_upd", coalesce(col("existing.endcomforttime"), col("main.endcomforttime")))
      .withColumn("timezonemethod_upd", coalesce(col("existing.timezonemethod"), col("main.timezonemethod")))
      .select(
        col("ctn"),
        col("startcomforttime_upd"),
        col("endcomforttime_upd"),
        col("timezonemethod_upd")
      )

    val finalDF = updatedMainDF.union(newRecords)

    resultDf
      .repartition(1)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableNbaAvailableTime)

    finalDF
      .repartition(1)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto("nba_engine.nba_available_time_increment")
  } else {
    spark.stop()
  }


}
