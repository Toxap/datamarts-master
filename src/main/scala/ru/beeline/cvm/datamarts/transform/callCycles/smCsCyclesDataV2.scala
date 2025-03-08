package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._
object smCsCyclesDataV2 extends Load {

  val SmCsBase22 = atsSmCsBase22
  val SmCsCyclesFull = atsSmCsCyclesFull

  val dataA = SmCsCyclesFull
    .select(col("subs_key"),
      col("ban_key"),
      col("subscriber_sk"),
      col("first_ctn"),
      col("first_ban"),
      col("time_key"),
      col("time_key_src"),
      col("cycle_start"),
      col("cycle_end"),
      col("cycle_duration"),
      col("prev_cycle_start"),
      col("prev_cycle_end"),
      col("prev_cycle_days"),
      col("next_cycle_start"),
      col("next_cycle_end"),
      col("next_cycle_days"));

  val dataB = SmCsBase22
    .select(
      col("subscriber_sk").as("b_subscriber_sk"),
      to_date(col("call_start_time"), "yyyy-MM-dd").as("b_call_start_time"),
      col("camp_type"),
      col("camp_FMC_flg"),
      col("call_result"),
      col("call_duration"));

  val dataC = dataA
    .join(dataB,
      col("subscriber_sk") === col("b_subscriber_sk") &&
        col("b_call_start_time").between(col("cycle_start"),
          col("cycle_end")), "left");

  val dataTableResult = dataC
    .groupBy("subs_key",
      "ban_key",
      "subscriber_sk",
      "first_ctn",
      "first_ban",
      "time_key",
      "time_key_src",
      "cycle_start",
      "cycle_end",
      "cycle_duration",
      "prev_cycle_start",
      "prev_cycle_end",
      "prev_cycle_days",
      "next_cycle_start",
      "next_cycle_end",
      "next_cycle_days")
    .agg(
      concat(
        when(sum(when(col("camp_type") === "A. Best Tariff Offer", 1).otherwise(0)) > 0, "A").otherwise(""),
        when(sum(when(col("camp_type") === "B. Overspend Data", 1).otherwise(0)) > 0, "B1").otherwise(""),
        when(sum(when(col("camp_type") === "B. Overspend  MG", 1).otherwise(0)) > 0, "B2").otherwise(""),
        when(sum(when(col("camp_type") === "B. Overspend Voice", 1).otherwise(0)) > 0, "B3").otherwise(""),
        when(sum(when(col("camp_type") === "C. MNP Port Out", 1).otherwise(0)) > 0, "C").otherwise(""),
        when(sum(when(col("camp_type") === "D. Single Use", 1).otherwise(0)) > 0, "D").otherwise(""),
        when(sum(when(col("camp_type") === "E. X-Sell", 1).otherwise(0)) > 0, "E").otherwise(""),
        when(sum(when(col("camp_type") === "F. Flat2Bundle", 1).otherwise(0)) > 0, "F").otherwise(""),
        when(sum(when(col("camp_type") === "G. Up-sell", 1).otherwise(0)) > 0, "G").otherwise(""),
        when(sum(when(col("camp_type") === "H. How Are You", 1).otherwise(0)) > 0, "H").otherwise(""),
        when(sum(when(col("camp_type") === "I. MS&LE", 1).otherwise(0)) > 0, "I").otherwise(""),
        when(sum(when(col("camp_type") === "J. Retention", 1).otherwise(0)) > 0, "J").otherwise(""),
        when(sum(when(col("camp_type") === "K. Reactivation", 1).otherwise(0)) > 0, "K").otherwise(""),
        when(sum(when(col("camp_type") === "L. Technica", 1).otherwise(0)) > 0, "L").otherwise(""),
        when(sum(when(col("camp_type") === "M. Prevention", 1).otherwise(0)) > 0, "M").otherwise(""),
        when(sum(when(col("camp_type") === "N. Information", 1).otherwise(0)) > 0, "N").otherwise(""),
        when(sum(when(col("camp_type") === "O. Down-sell", 1).otherwise(0)) > 0, "O").otherwise(""),
        when(sum(when(col("camp_type") === "P. Win Back", 1).otherwise(0)) > 0, "P").otherwise(""),
        when(sum(when(col("camp_type") === "Q. LE", 1).otherwise(0)) > 0, "Q").otherwise(""),
        when(sum(when(col("camp_type") === "R. Test", 1).otherwise(0)) > 0, "R").otherwise(""),
        when(sum(when(col("camp_type") === "S. Other", 1).otherwise(0)) > 0, "S").otherwise(""))
        .as("cycle_use_case"),

      when(sum(when(col("camp_FMC_flg") === "Yes", 1).otherwise(0)) > 0, 1)
        .otherwise(0).as("camp_FMC_flg"),

      count(col("subs_key")).as("calls_cnt"),

      sum(when(col("call_result").isin("Абонент не ответил", "Автоответчик"), 0)
        .otherwise(col("call_duration"))).as("calls_duration"),

      when(sum(when(col("call_result") === "Будет", 1).otherwise(0)) > 0, "A. Yes")
        .when(sum(when(col("call_result") === "Не будет", 1).otherwise(0)) > 0, "B. No")
        .when(sum(when(col("call_result") === "Отказ от ответа", 1).otherwise(0)) > 0, "C. Refusal")
        .when(sum(when(col("call_result") === "Блокировка", 1).otherwise(0)) > 0, "D. Block")
        .when(sum(when(col("call_result") === "Не знаком, нет времени", 1).otherwise(0)) > 0, "E. No time")
        .when(sum(when(col("call_result").isin("Абонент не ответил", "Автоответчик"), 1).otherwise(0)) > 0, "F. No answer")
        .otherwise("G. Other").as("cycle_result"),

      sum(when(col("call_result").isin("Абонент не ответил", "Автоответчик"), 1)
        .otherwise(0)).as("A_no_answer_cnt"),
      sum(when(col("call_result") === "Не знаком, нет времени", 1)
        .otherwise(0)).as("B_no_time_cnt"),
      sum(when(col("call_result") === "Не будет", 1)
        .otherwise(0)).as("C_no_cnt"),
      sum(when(col("call_result") === "Будет", 1)
        .otherwise(0)).as("D_yes_cnt"),
      sum(when(col("call_result") === "Отказ от ответа", 1)
        .otherwise(0)).as("E_refusal_cnt"),
      sum(when(col("call_result") === "Блокировка", 1)
        .otherwise(0)).as("F_block_cnt"),

      sum(when(col("call_result").isin("Абонент не ответил", "Автоответчик"), 0)
        .otherwise(0)).as("A_no_answer_duration"),
      sum(when(col("call_result") === "Не знаком, нет времени", col("call_duration"))
        .otherwise(0)).as("B_no_time_duration"),
      sum(when(col("call_result") === "Не будет", col("call_duration"))
        .otherwise(0)).as("C_no_duration"),
      sum(when(col("call_result") === "Будет", col("call_duration"))
        .otherwise(0)).as("D_yes_duration"),
      sum(when(col("call_result") === "Отказ от ответа", col("call_duration"))
        .otherwise(0)).as("E_refusal_duration"),
      sum(when(col("call_result") === "Блокировка", col("call_duration"))
        .otherwise(0)).as("F_block_duration"),

      when(sum(when(!col("call_result").isin("Абонент не ответил", "Отказ от ответа", "Автоответчик") &&
        (col("call_duration") > 45), 1).otherwise(0)) > 0, 1)
        .otherwise(0).as("flg_3"),

      when(sum(when(!col("call_result").isin("Абонент не ответил", "Автоответчик") &&
        (col("call_duration") <= 45) && (col("call_duration") > 5), 1)
        .when(col("call_result") === "Отказ от ответа", 1)
        .otherwise(0)) > 0, 1).otherwise(0).as("flg_2"));

  dataTableResult
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableSmCsCyclesDataV2);

}
