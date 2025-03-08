package ru.beeline.cvm.datamarts.transform.coreClients7Days

import org.apache.spark.sql.functions.{col, to_date}

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object InflowDaily7Days extends CustomParams {
  val inflowDay = spark.read.table("nba_engine.am_core_clients_sample_base")
  def generateDaysBetween(startDateStr: String, endDateStr: String): IndexedSeq[LocalDate] = {
    val start = LocalDate.parse(startDateStr)
    val end = LocalDate.parse(endDateStr)
    val between = ChronoUnit.DAYS.between(start, end).toInt
    for (monthPlus <- 0 to between)
      yield start.plusDays(monthPlus)
  }

  val data = generateDaysBetween(
    LOAD_DATE_YYYY_MM_DD_14,
    LOAD_DATE_YYYY_MM_DD_8
  )
    .map(_.toString)
  val days = List.range(0, 7)
  val pairs = days.zip(data)
  val columns = Seq("day", "data")
  val dateDF = spark.createDataFrame(pairs).toDF(columns: _*)
    .withColumn("data", to_date(col("data"), "yyyy-MM-dd"))

  val inflowDaily7Days = inflowDay.crossJoin(dateDF)

  inflowDaily7Days
    .write
    .mode("overwrite")
    .format("orc")
    .saveAsTable("nba_engine.am_core_clients_sample_base_7d")

}
