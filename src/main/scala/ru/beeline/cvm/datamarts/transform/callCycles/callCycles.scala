package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import scala.util.matching.Regex

class callCycles extends Load {


  val SmCsCyclesData2 = atsSmCsCyclesData2
  val SmCsCyclesPrevsV2 = atsSmCsCyclesPrevsV2


  val data3A = SmCsCyclesData2;

  val data3B = SmCsCyclesPrevsV2;

  val data3TableResult = data3A
    .join(data3B, Seq("time_key_src", "subscriber_sk", "cycle_start"), "left");


  val resultDf = data3TableResult
    .select(
      col("subs_key"),
      col("ban_key").cast("bigint").as("ban_key"),
      col("subscriber_sk"),
      col("first_ctn"),
      col("first_ban"),
      col("time_key"),
      col("cycle_start").cast("date"),
      col("cycle_end"),
      col("cycle_duration").cast("int"),
      col("calls_cnt"),
      col("calls_duration"),
      col("camp_FMC_flg"),
      col("cycle_use_case"),
      col("cycle_result"),
      col("flg_3").cast("int"),
      col("flg_2"),
      col("target"),
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
      col("F_block_duration"),
      col("prev_cycle_start").cast("date"),
      col("prev_cycle_end"),
      col("prev_cycle_days").cast("int"),
      col("prev_cycle_duration"),
      col("prev_cycle_calls_cnt"),
      col("prev_cycle_calls_duration"),
      col("prev_cycle_use_case"),
      col("prev_cycle_result"),
      col("prev_cycle_A_no_answer_cnt").cast("long"),
      col("prev_cycle_B_no_time_cnt"),
      col("prev_cycle_C_no_cnt"),
      col("prev_cycle_D_yes_cnt"),
      col("prev_cycle_E_refusal_cnt"),
      col("prev_cycle_F_block_cnt"),
      col("prev_cycle_A_no_answer_duration"),
      col("prev_cycle_B_no_time_duration"),
      col("prev_cycle_C_no_duration"),
      col("prev_cycle_D_yes_duration"),
      col("prev_cycle_E_refusal_duration"),
      col("prev_cycle_F_block_duration"),
      col("prev_cycle_target"),
      col("prev_cycles_cnt"),
      col("prev_cycles_duration"),
      col("prev_cycles_calls_cnt"),
      col("prev_cycles_calls_duration"),
      col("prev_cycles_intervals"),
      col("prev_cycles_A_no_answer_cnt"),
      col("prev_cycles_B_no_time_cnt"),
      col("prev_cycles_C_no_cnt"),
      col("prev_cycles_D_yes_cnt"),
      col("prev_cycles_E_refusal_cnt"),
      col("prev_cycles_F_block_cnt"),
      col("prev_cycles_A_no_answer_duration"),
      col("prev_cycles_B_no_time_duration"),
      col("prev_cycles_C_no_duration"),
      col("prev_cycles_D_yes_duration"),
      col("prev_cycles_E_refusal_duration"),
      col("prev_cycles_F_block_duration"),
      col("prev_cycles_A_no_answer_pct"),
      col("prev_cycles_B_no_time_pct"),
      col("prev_cycles_C_no_pct"),
      col("prev_cycles_D_yes_pct"),
      col("prev_cycles_E_refusal_pct"),
      col("prev_cycles_F_block_pct"),
      col("time_key_src").cast("date")
    );
}
