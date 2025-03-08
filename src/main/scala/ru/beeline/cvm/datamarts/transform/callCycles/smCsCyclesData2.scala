package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._
object smCsCyclesData2 extends Load{

  val SmCsCyclesDataV2 = atsSmCsCyclesDataV2

  val data2A = SmCsCyclesDataV2
    .withColumn("target",
      when(col("flg_3") === 1, 3)
        .when(col("flg_2") === 1, 2)
        .when(col("A_no_answer_cnt") === col("calls_cnt"), 1)
        .otherwise(-1));

  val data2B = SmCsCyclesDataV2
    .select(col("subscriber_sk"),
      col("cycle_start").as("prev_cycle_start"),
      col("cycle_duration").as("prev_cycle_duration"),
      col("calls_cnt").as("prev_cycle_calls_cnt"),
      col("calls_duration").as("prev_cycle_calls_duration"),
      col("cycle_use_case").as("prev_cycle_use_case"),
      col("cycle_result").as("prev_cycle_result"),
      col("A_no_answer_cnt").as("prev_cycle_A_no_answer_cnt"),
      col("B_no_time_cnt").as("prev_cycle_B_no_time_cnt"),
      col("C_no_cnt").as("prev_cycle_C_no_cnt"),
      col("D_yes_cnt").as("prev_cycle_D_yes_cnt"),
      col("E_refusal_cnt").as("prev_cycle_E_refusal_cnt"),
      col("F_block_cnt").as("prev_cycle_F_block_cnt"),
      col("A_no_answer_duration").as("prev_cycle_A_no_answer_duration"),
      col("B_no_time_duration").as("prev_cycle_B_no_time_duration"),
      col("C_no_duration").as("prev_cycle_C_no_duration"),
      col("D_yes_duration").as("prev_cycle_D_yes_duration"),
      col("E_refusal_duration").as("prev_cycle_E_refusal_duration"),
      col("F_block_duration").as("prev_cycle_F_block_duration"),
      when(col("flg_3") === 1, 3)
        .when(col("flg_2") === 1, 2)
        .when(col("A_no_answer_cnt") === col("calls_cnt"), 1)
        .otherwise(-1).as("prev_cycle_target"));

  val data2TableResult = data2A
    .join(data2B, Seq("subscriber_sk", "prev_cycle_start"), "left");

  data2TableResult
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableSmCsCyclesData2);

}
