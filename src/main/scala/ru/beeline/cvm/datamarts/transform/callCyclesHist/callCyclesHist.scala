package ru.beeline.cvm.datamarts.transform.callCyclesHist

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class callCyclesHist extends Load {

  def getBmtBase: DataFrame = {
    getDimCtnBan
      .filter(col("time_key_src") === REPORT_DATE_MONTH_BEGIN)
      .select(
        col("subscriber_sk")
      )

  }

  def getCallCyclesDF: DataFrame = {
    getCallCycles
      .filter(col("time_key_src").between(REPORT_DATE_MINUS_300_START_MONTHS, REPORT_DATE_MONTH_BEGIN))
      .filter(col("target") =!= -1)
      .withColumn("report_date", lit(REPORT_DATE))
      .filter(col("cycle_start").between(lit(REPORT_DATE_MINUS_300), lit(REPORT_DATE_MINUS_DAY)))
  }

  def getCtnCallCyclesDF: DataFrame = {
    getBmtBase
      .join(getCallCyclesDF, Seq("subscriber_sk"))
  }

  def getBasePrev: DataFrame = {
    val cycles = getCtnCallCyclesDF
      .withColumn("rn", row_number().over(Window.partitionBy(col("subscriber_sk")).orderBy(col("cycle_start").desc)))
      .filter(col("rn") === 1)
      .select(
        col("subscriber_sk"),
        col("subs_key"),
        col("ban_key"),
        col("first_ctn"),
        col("first_ban"),
        col("report_date"),
        datediff(col("report_date"), col("cycle_end")).as("prev_cycle_datediff"),
        lit(1).as("was_prev_cycle_300d_ind"),
        col("cycle_start").as("prev_cycle_start"),
        col("cycle_end").as("prev_cycle_end"),
        col("cycle_duration").as("prev_cycle_duration"),
        col("calls_cnt").as("prev_cycle_calls_cnt"),
        col("calls_duration").as("prev_cycle_calls_duration"),
        col("camp_fmc_flg").as("prev_cycle_camp_fmc_flg"),
        col("cycle_use_case").as("prev_cycle_use_case"),
        col("cycle_result").as("prev_cycle_result"),
        col("target").as("prev_cycle_target"),
        col("a_no_answer_cnt").as("prev_cycle_a_no_answer_cnt"),
        col("b_no_time_cnt").as("prev_cycle_b_no_time_cnt"),
        col("c_no_cnt").as("prev_cycle_c_no_cnt"),
        col("d_yes_cnt").as("prev_cycle_d_yes_cnt"),
        col("e_refusal_cnt").as("prev_cycle_e_refusal_cnt"),
        col("f_block_cnt").as("prev_cycle_f_block_cnt"),
        col("a_no_answer_duration").as("prev_cycle_a_no_answer_duration"),
        col("b_no_time_duration").as("prev_cycle_b_no_time_duration"),
        col("c_no_duration").as("prev_cycle_c_no_duration"),
        col("d_yes_duration").as("prev_cycle_d_yes_duration"),
        col("e_refusal_duration").as("prev_cycle_e_refusal_duration"),
        col("f_block_duration").as("prev_cycle_f_block_duration"),
        col("time_key_src")
      )

    getCtnCallCyclesDF.select(col("subscriber_sk")).distinct
      .join(cycles, Seq("subscriber_sk"), "left")
  }

  def getPrevCyclesDF: DataFrame = {
    getCtnCallCyclesDF
      .groupBy(col("subscriber_sk"))
      .agg(sum(when(col("subs_key").isNotNull, 1).otherwise(0)).as("prev_cycles_cnt"),
        when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
          sum(col("cycle_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_duration"),
        when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
          sum(col("calls_cnt"))).otherwise(lit(null).cast(IntegerType)).as("prev_cycles_calls_cnt"),
        when(sum(when(col("subs_key").isNotNull, 1).otherwise(0)) > 0,
          sum(col("calls_duration"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_calls_duration"),
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
          sum(col("F_block_cnt")) / sum(col("calls_cnt"))).otherwise(lit(null).cast(DoubleType)).as("prev_cycles_F_block_pct")
      )
  }

  def getResultDF: DataFrame = {
    getBasePrev
      .join(getPrevCyclesDF, Seq("subscriber_sk"), "left")
      .select(
        col("subscriber_sk"),
        col("subs_key"),
        col("ban_key"),
        col("first_ctn"),
        col("first_ban"),
        col("prev_cycle_datediff"),
        col("was_prev_cycle_300d_ind"),
        col("prev_cycle_start"),
        col("prev_cycle_end"),
        col("prev_cycle_duration"),
        col("prev_cycle_calls_cnt"),
        col("prev_cycle_calls_duration"),
        col("prev_cycle_camp_fmc_flg"),
        col("prev_cycle_use_case"),
        col("prev_cycle_result"),
        col("prev_cycle_target"),
        col("prev_cycle_a_no_answer_cnt"),
        col("prev_cycle_b_no_time_cnt"),
        col("prev_cycle_c_no_cnt"),
        col("prev_cycle_d_yes_cnt"),
        col("prev_cycle_e_refusal_cnt"),
        col("prev_cycle_f_block_cnt"),
        col("prev_cycle_a_no_answer_duration"),
        col("prev_cycle_b_no_time_duration"),
        col("prev_cycle_c_no_duration"),
        col("prev_cycle_d_yes_duration"),
        col("prev_cycle_e_refusal_duration"),
        col("prev_cycle_f_block_duration"),
        col("prev_cycles_cnt"),
        col("prev_cycles_duration"),
        col("prev_cycles_calls_cnt"),
        col("prev_cycles_calls_duration"),
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
        col("time_key_src"),
        col("report_date").cast("date").as("report_date")
      )
  }

}
