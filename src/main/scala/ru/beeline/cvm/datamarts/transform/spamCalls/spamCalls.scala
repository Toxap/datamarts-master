package ru.beeline.cvm.datamarts.transform.spamCalls

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class spamCalls extends Load {

  val first = tableDmMobileAttr;
  val baseB2c22 = baseB2c22Table;
  val baseB2c23 = baseB2c23Table;
  val incomeCallsTable = incomeCallsTableData;
  val spamBaseTable= spamBaseTableData;

  val baseB2c2022 = baseB2c22
    .filter(col("test_price_plan_flg") === 0)
    .filter(col("test_ban_flg") === 0)
    .filter(col("test_account_flg") === 0)
    .filter(!col("segment_cd").isin("-99","TST"))
    .filter(col("business_type_cd") === "B2C")
    .withColumn("time_key_src", trunc(col("CALENDAR_DT"), "month"))
    .filter(col("time_key_src").between(MIN_SCORE_DATE_MINUS_TWO_YEAR,MAX_SCORE_DATE_MINUS_TWO_YEAR))
    .select(
      col("subscriber_num").as("subs_key"),
      col("ban_num").as("ban_key"),
      col("subscriber_sk"),
      col("subscriber_status_cd"),
      col("subscriber_base_flg"),
      col("time_key_src"),
      col("CALENDAR_DT"));

  val window = Window.partitionBy("subscriber_sk", "time_key_src");

  val baseB2c2022W = baseB2c2022
    .withColumn("last_date", max("CALENDAR_DT").over(window))
    .withColumn("last_date_flg", when(col("last_date") === col("CALENDAR_DT"), 1).otherwise(0));

  val baseB2c2022Fin = baseB2c2022W
    .filter(col("last_date_flg") === 1)
    .select(
      "subs_key",
      "ban_key",
      "subscriber_sk",
      "subscriber_status_cd",
      "subscriber_base_flg",
      "time_key_src"
    );

  val baseB2c2023 = baseB2c23
    .filter(col("test_price_plan_flg") === 0)
    .filter(col("test_ban_flg") === 0)
    .filter(col("test_account_flg") === 0)
    .filter(!col("segment_cd").isin("-99","TST"))
    .filter(col("business_type_cd") === "B2C")
    .filter(col("CALENDAR_DT") >= SCORE_DATE_MINUS_ONE_YEAR)
    .select(
      col("subscriber_num").as("subs_key"),
      col("ban_num").as("ban_key"),
      col("subscriber_sk"),
      col("subscriber_status_cd"),
      col("subscriber_base_flg"),
      col("CALENDAR_DT").as("time_key_src")
    );

  val baseB2c = baseB2c2022Fin.union(baseB2c2023);

  val firstBase = first
    .select(
      col("subscriber_sk"),
      col("first_ban"),
      col("first_subscriber").as("first_ctn")
    );

  val baseB2cFirst = baseB2c
    .join(firstBase,Seq("subscriber_sk"),"left");

  val spamBase = spamBaseTable
    .withColumn("income_ctn", trim(col("ctn")))
    .filter(col("score") >= 0.85)
    .withColumn("spam_ind", lit(1))
    .withColumn("time_key_day", to_date(col("time_key"),"yyyy-MM-dd"))
    .select(
      col("income_ctn"),
      col("time_key_day"),
      col("spam_ind"))
    .dropDuplicates();

  val incomeCalls =  incomeCallsTable
    .withColumn("subs_key", trim(col("ctn")))
    .withColumn("income_ctn", trim(col("num_a")))
    .filter(col("direction") === 1)
    .withColumn("time_key_src", to_date(col("time_key"), "yyyy-MM"))
    .withColumn("time_key_day", to_date(col("time_key"), "yyyy-MM-dd"))
    .filter(col("time_key_src") >= "2023-11-01")
    .select(col("subs_key"),
      col("income_ctn"),
      col("time_key_src"),
      col("time_key_day"),
      col("has_forwarding"),
      col("duration"));

  val incomeCallsB2c = incomeCalls
    .join(baseB2cFirst, Seq("subs_key", "time_key_src"), "inner");

  val baseIncomeSpamCalls = incomeCallsB2c
    .join(spamBase, Seq("income_ctn","time_key_day"), "inner");

  val daysInMonth = baseIncomeSpamCalls
    .groupBy("time_key_src")
    .agg(countDistinct(col("time_key_day")).as("days_in_month"));

  val baseIncomeSpamCallsD = baseIncomeSpamCalls
    .join(daysInMonth, Seq("time_key_src"), "Left");

  val incomeSpamCallsGr = baseIncomeSpamCallsD
    .groupBy("subs_key", "time_key_src", "days_in_month")
    .agg(
      countDistinct(col("time_key_day")).as("days_with_spam_cnt"),
      sum(col("spam_ind")).as("spam_calls_cnt"),
      countDistinct(col("income_ctn")).as("nuniq_spammers"),
      sum(col("has_forwarding")).as("has_forwarding_cnt"),
      round(mean("duration"), 2).as("avg_dur_spam_call"),
      max(col("duration")).as("max_dur_spam_call"),
      min(col("time_key_day")).as("min_date_of_call"),
      max(col("time_key_day")).as("max_date_of_call"));

  val incomeSpamCallsFin = incomeSpamCallsGr
    .withColumn("days_with_spam_cnt_norm", round(col("days_with_spam_cnt") / col("days_in_month"), 3))
    .withColumn("spam_calls_cnt_norm",round(col("spam_calls_cnt") / col("days_in_month"), 3))
    .withColumn("nuniq_spammers_norm",round(col("nuniq_spammers") / col("days_in_month"), 3))
    .withColumn("has_forwarding_cnt_norm",round(col("has_forwarding_cnt") / col("days_in_month"), 3))
    .withColumn("report_dt",lit(LOAD_DATE_YYYY_MM_DD))
    .select(
      "subs_key",
      "time_key_src",
      "days_in_month",
      "days_with_spam_cnt",
      "spam_calls_cnt",
      "nuniq_spammers",
      "has_forwarding_cnt",
      "avg_dur_spam_call",
      "max_dur_spam_call",
      "min_date_of_call",
      "max_date_of_call",
      "days_with_spam_cnt_norm",
      "spam_calls_cnt_norm",
      "nuniq_spammers_norm",
      "has_forwarding_cnt_norm",
      "report_dt"
    )


}
