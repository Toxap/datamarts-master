package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object сallCyclesPrevsV2 extends Load {

  val SmCsCyclesData2 = atsSmCsCyclesData2

  val dataPrevsA = SmCsCyclesData2
    .select(col("subscriber_sk"),
      col("cycle_start"),
      col("time_key_src"));

  val dataPrevsB = SmCsCyclesData2
    .select(col("subscriber_sk").as("b_subscriber_sk"),
      col("cycle_start").as("b_cycle_start"),
      col("subs_key"),
      col("cycle_duration"),
      col("calls_cnt"),
      col("calls_duration"),
      col("prev_cycle_days"),
      col("next_cycle_days"),
      col("A_no_answer_cnt"),
      col("B_no_time_cnt"),
      col("C_no_cnt"),
      col("D_yes_cnt"),
      col("E_refusal_cnt"),
      col("F_block_cnt"),
      col("A_no_answer_duration"),
      col("B_no_time_duration"),
      col("C_no_duration"),
      col("D_yes_duration"),
      col("E_refusal_duration"),
      col("F_block_duration"));

  val dataPrevsC = dataPrevsA
    .join(dataPrevsB,
      col("subscriber_sk") === col("b_subscriber_sk") &&
        col("b_cycle_start").between(date_add(col("cycle_start"), -270), date_add(col("cycle_start"), -1)),
      "left");

  val dataPrevsTableResult = dataPrevsC
    .groupBy("subscriber_sk", "cycle_start", "time_key_src")
    .agg(sum(when(col("subs_key").isNotNull, 1).otherwise(0)).as("prev_cycles_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("cycle_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("calls_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_calls_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("calls_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_calls_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("next_cycle_days")) / count(col("subs_key"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_intervals"),

      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("A_no_answer_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_A_no_answer_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("B_no_time_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_B_no_time_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("C_no_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_C_no_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("D_yes_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_D_yes_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("E_refusal_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_E_refusal_cnt"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("F_block_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_F_block_cnt"),

      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("A_no_answer_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_A_no_answer_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("B_no_time_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_B_no_time_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("C_no_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_C_no_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("D_yes_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_D_yes_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("E_refusal_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_E_refusal_duration"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("F_block_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_F_block_duration"),


      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0, sum(col("A_no_answer_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_A_no_answer_pct"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("B_no_time_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_B_no_time_pct"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("C_no_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_C_no_pct"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("D_yes_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_D_yes_pct"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("E_refusal_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_E_refusal_pct"),
      when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
        sum(col("F_block_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_F_block_pct"));

  dataPrevsTableResult
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableCallCyclesPrevsV2);

}
