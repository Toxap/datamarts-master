package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import scala.util.matching.Regex

object callCyclesPrev2 extends Load{

  val SmCsCycles2 = atsSmCsCycles2

  val prevA = SmCsCycles2
    .select(
      col("subscriber_sk"),
      col("time_key_src"),
      col("cycle_start"));

  val prevB = SmCsCycles2
    .select(
      col("subscriber_sk").as("b_subscriber_sk"),
      col("cycle_start").as("prev_cycle_start"),
      col("cycle_end").as("prev_cycle_end"));

  val prevC = prevA
    .join(prevB,
      col("subscriber_sk") === col("b_subscriber_sk") &&
        col("prev_cycle_start").between(date_add(col("cycle_start"), -270), date_add(col("cycle_start"), -1)),
      "left");

  val prevTableResult = prevC
    .groupBy("subscriber_sk",
      "time_key_src",
      "cycle_start")
    .agg(max("prev_cycle_start").as("prev_cycle_start"),
      max("prev_cycle_end").as("prev_cycle_end"));

  val resultWithPrevCycleDays = prevTableResult
    .select(
      col("subscriber_sk"),
      col("time_key_src"),
      col("cycle_start"),
      col("prev_cycle_start"),
      col("prev_cycle_end"),
      datediff(
        to_date(col("cycle_start")),
        to_date(col("prev_cycle_end"))
      ).alias("prev_cycle_days"));



  resultWithPrevCycleDays
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableCallCyclesPrev2);

}
