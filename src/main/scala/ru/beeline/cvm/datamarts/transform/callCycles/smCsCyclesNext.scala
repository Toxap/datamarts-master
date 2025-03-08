package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._

object smCsCyclesNext extends Load {

  val SmCsCycles2 = atsSmCsCycles2

  val nextA = SmCsCycles2
    .select(col("subscriber_sk"),
      col("time_key"),
      col("cycle_start"),
      col("cycle_end"));

  val nextB = SmCsCycles2
    .select(
      col("subscriber_sk").as("b_subscriber_sk"),
      col("cycle_start").as("next_cycle_start"),
      col("cycle_end").as("next_cycle_end"));

  val nextC = nextA
    .join(nextB,
        col("subscriber_sk") === col("b_subscriber_sk") &&
        col("next_cycle_start").between(date_add(col("cycle_end"), 1), date_add(col("cycle_end"), 180)),
      "left");

  val nextTableResult = nextC
    .groupBy("subscriber_sk",
      "time_key",
      "cycle_start",
      "cycle_end")
    .agg(min("next_cycle_start").as("next_cycle_start"),
      min("next_cycle_end").as("next_cycle_end"));

  val resultWithNextCycleDays = nextTableResult
    .select(
      col("subscriber_sk"),
      col("time_key"),
      col("cycle_start"),
      col("next_cycle_start"),
      col("next_cycle_end"),
      datediff(
        to_date(col("next_cycle_start")),
        to_date(col("cycle_end"))
      ).alias("next_cycle_days"));

  resultWithNextCycleDays
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key")
    .saveAsTable(TableSmCsCyclesNext);

}
