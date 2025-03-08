package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object smCsCycles2 extends Load {

  val SmCsBase22 = atsSmCsBase22

  val cyclesA = SmCsBase22
    .filter(coalesce(col("prev_call_days"), lit(8)) > 7)
    .withColumn("cycle_start", to_date(col("call_start_time"), "yyyy-MM-dd"))
    .select(col("subs_key"),
      col("ban_key"),
      col("subscriber_sk"),
      col("first_ctn"),
      col("first_ban"),
      col("time_key"),
      col("time_key_src"),
      col("cycle_start"))
    .dropDuplicates();

  val cyclesB = SmCsBase22
    .filter(coalesce(col("next_call_days"), lit(8)) > 7)
    .withColumn("cycle_end", to_date(col("call_start_time"), "yyyy-MM-dd"))
    .select(col("subscriber_sk"),
      col("cycle_end"))
    .dropDuplicates();

  val cyclesTableResult = cyclesA
    .withColumn("r", row_number().over(Window.partitionBy("subscriber_sk").orderBy("cycle_start")))
    .join(cyclesB
      .withColumn("r", row_number().over(Window.partitionBy("subscriber_sk").orderBy("cycle_end"))),
      Seq("subscriber_sk", "r"), "Left");

  val colResultCyc = Seq("subs_key",
    "ban_key",
    "subscriber_sk",
    "first_ctn",
    "first_ban",
    "time_key",
    "time_key_src",
    "cycle_start",
    "cycle_end",
    "cycle_duration");

  val resultWithCycleDuration = cyclesTableResult
    .withColumn(
      "cycle_duration",
      datediff(to_date(col("cycle_end")), to_date(col("cycle_start"))) + 1)
    .select(colResultCyc.map(col): _*);

  try {
    resultWithCycleDuration
      .write
      .mode("overwrite")
      .format("orc")
      .partitionBy("time_key")
      .saveAsTable(TableSmCsCycles2);
  }

}
