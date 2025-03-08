package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object smCsBase22 extends Load {

  val SmCsBase2 = atsSmCsBase2

  val callA = SmCsBase2
    .filter(col("subscriber_sk").isNotNull)
    .withColumn("r_call",
      row_number().over(Window.partitionBy("subscriber_sk").orderBy("call_start_time")));

  val nextCallB = SmCsBase2
    .filter(col("subscriber_sk").isNotNull)
    .withColumn("r_call",
      row_number().over(Window.partitionBy("subscriber_sk")
        .orderBy("call_start_time")) + 1);

  val prevCallC = SmCsBase2
    .filter(col("subscriber_sk").isNotNull)
    .withColumn("r_call",
      row_number().over(Window.partitionBy("subscriber_sk")
        .orderBy("call_start_time")) - 1);

  val tableCallResult = callA
    .join(nextCallB
      .select(
        col("subscriber_sk"),
        col("r_call"),
        col("call_start_time").as("prev_call_start_time")),
      Seq("subscriber_sk", "r_call"), "left")

    .join(prevCallC
      .select(
        col("subscriber_sk"),
        col("r_call"),
        col("call_start_time").as("next_call_start_time")),
      Seq("subscriber_sk", "r_call"), "left");

  val colResultC = Seq("subs_key",
    "ban_key",
    "subscriber_sk",
    "first_ctn",
    "first_ban",
    "time_key",
    "time_key_src",
    "call_start_time",
    "call_end_time",
    "call_duration",
    "call_result",
    "camp_nm_full",
    "camp_type",
    "camp_FMC_flg",
    "prev_call_days",
    "next_call_days");

  val resultWithDates = tableCallResult
    .withColumn("prev_call_days",
      datediff(to_date(col("call_start_time")),
        to_date(col("prev_call_start_time"))))
    .withColumn("next_call_days",
      datediff(to_date(col("next_call_start_time")),
        to_date(col("call_start_time"))))
    .select(colResultC.map(col): _*);

  resultWithDates
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key")
    .saveAsTable(TableSmCsBase22);

}
