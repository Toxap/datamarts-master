package ru.beeline.cvm.datamarts.transform.SubPreset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, lag, lit, max, month, rand, round, row_number, sum, when, asc, explode}

class SubPreset extends Load {

  val dimSoc = getDimSoc;
  val dmSubs = getDmSubs;
  val dmSubsSoc = getSubsSoc(dimSoc);
  val ppParamFull = getPpParam(listPricePlanKey);
  val ppParamEast = getPpParamEast(listPricePlanKey);
  val ppParam = ppParamFull.union(ppParamEast);
  val voiceDataFillUp = getSocUpFill;
  val base = getBase;
  val family = getFamily;
  val usageSubs = spark.read.table("beemetrics.dm_mobile_subscriber_usage_daily")
    .filter(col("transaction_dt").between(SCORE_DATE_MINUS_3_MONTH, SCORE_DATE))
    .filter(col("ACCOUNT_TYPE_CD").isin(13, 341))
    .filter(col("ROAMING_CD").isin("H", "X"))
    .join(dmSubs, Seq("SUBSCRIBER_NUM", "BAN_NUM"), "right")
    .withColumn("month", month(col("transaction_dt")));

  val joinedData = usageSubs.as("usg")
    .join(family.as("rec"), usageSubs("SUBSCRIBER_NUM") === family("recipient"), "left_outer");

  val consumptionPerCTN = joinedData
    .withColumn("month", month(col("transaction_dt")))
    .groupBy(
      col("SUBSCRIBER_NUM").as("ctn"),
      col("BAN_NUM").as("ban"),
      col("SUBS_MARKET_CD").as("market_key"),
      col("month")
    )
    .agg(
      sum(when(
        col("ACTIVITY_CD") === "VOICE" && col("CALL_DIRECTION_CD") === 2,
        col("usage_amt") / 60 * 1.20)).as("total_voice_usage"),
      sum(when(
        col("ACTIVITY_CD") === "GPRS" && col("CALL_DIRECTION_CD") === 3,
        col("usage_amt") / 1024 / 1024 / 1024 * 1.20)).as("total_data_usage")
    )
    .groupBy(
      col("ctn"),
      col("ban"),
      col("market_key")
    )
    .agg(
      round(coalesce(max(col("total_voice_usage")), lit(0)), 2).as("voice_usage"),
      round(coalesce(max(col("total_data_usage")), lit(0)), 2).as("data_usage"));

  val consumptionPerRecipient = joinedData
    .withColumn("month", month(col("transaction_dt")))
    .groupBy(
      col("recipient"),
      col("BAN_NUM").as("ban"),
      col("SUBS_MARKET_CD").as("market_key"),
      col("month"))
    .agg(
      sum(when(
        col("ACTIVITY_CD") === "VOICE" && col("CALL_DIRECTION_CD") === 2,
        col("usage_amt") / 60 * 1.20)).as("total_voice_usage"),
      sum(when(
        col("ACTIVITY_CD") === "GPRS" && col("CALL_DIRECTION_CD") === 3,
        col("usage_amt") / 1024 / 1024 / 1024 * 1.20)).as("total_data_usage")
    )
    .groupBy(
      col("recipient"),
      col("ban"),
      col("market_key"))
    .agg(
      round(coalesce(max(col("total_voice_usage")), lit(0)), 2).as("voice_usage"),
      round(coalesce(max(col("total_data_usage")), lit(0)), 2).as("data_usage")
    );

  val df2 = family
    .join(consumptionPerRecipient, Seq("recipient"))
    .select(col("ctn"), col("recipient"), col("voice_usage").as("voice_usage_rec"), col("data_usage").as("data_usage_rec"));

  val finalConsumption = consumptionPerCTN.join(df2, Seq("ctn"), "outer")
    .groupBy("ctn", "voice_usage", "data_usage")
    .agg(
      sum(col("voice_usage_rec")).as("sum_voice_usage_res"),
      sum(col("data_usage_rec")).as("sum_data_usage_res")
    )
    .withColumn("voice_usage_rec", col("voice_usage") + col("sum_voice_usage_res"))
    .withColumn("data_usage_rec", col("data_usage") + col("sum_data_usage_res"))
    .filter(col("voice_usage_rec").isNotNull && col("data_usage_rec").isNotNull)
    .select(col("ctn"), col("voice_usage_rec"), col("data_usage_rec"))
    .withColumn("voice_usage_rec",round(col("voice_usage_rec"), 2))
    .withColumn("data_usage_rec",round(col("data_usage_rec"), 2));

  val usageFull = usageSubs
    .groupBy(
      col("SUBSCRIBER_NUM").as("ctn"),
      col("BAN_NUM").as("ban"),
      col("SUBS_MARKET_CD").as("market_key"),
      col("month")
    )
    .agg(
      sum(when(
        col("ACTIVITY_CD") === "VOICE" && col("CALL_DIRECTION_CD") === 2,
        coalesce(col("usage_amt"), lit(0)) / 60 * 1.20 )).as("total_voice_usage"),
      sum(when(
        col("ACTIVITY_CD") === "GPRS" && col("CALL_DIRECTION_CD") === 3,
        coalesce(col("usage_amt"), lit(0)) / 1024 / 1024 / 1024 * 1.20 )).as("total_data_usage"))
    .groupBy(
      col("ctn"),
      col("ban"),
      col("market_key"))
    .agg(
      round(coalesce(max(col("total_voice_usage")), lit(0)), 2).as("voice_usage"),
      round(coalesce(max(col("total_data_usage")), lit(0)), 2).as("data_usage"))
    .join(finalConsumption, Seq("ctn"), "left")
    .withColumn("voice_usage", when(col("voice_usage_rec").isNotNull, col("voice_usage_rec")).otherwise(col("voice_usage")))
    .withColumn("data_usage", when(col("data_usage_rec").isNotNull, col("data_usage_rec")).otherwise(col("data_usage")))
    .withColumn("data_usage_pred", when(
      col("data_usage") >= 60, lit(60))
      .otherwise(
        when(col("data_usage") <= 0, lit(0))
          .otherwise(col("data_usage"))))
    .withColumn("voice_usage_pred",
      when(col("voice_usage") >= 3000, lit(3000))
        .otherwise(
          when(col("voice_usage") <= 0, lit(0))
            .otherwise(col("voice_usage"))));

  val trParamData = ppParam
    .select(
      col("market_key"),
      col("count_gprs"))
    .distinct
    .withColumn("prev_gprs", coalesce(lag(col("count_gprs"), 1).over(Window.partitionBy("market_key")
      .orderBy("count_gprs")), lit(0)));

  val trParamVoice = ppParam
    .select(
      col("market_key"),
      col("cnt_voice_internal_local"),
      col("soc_voice"))
    .distinct
    .withColumn("prev_cnt_voice", coalesce(lag(col("cnt_voice_internal_local"), 1).over(Window.partitionBy("market_key")
      .orderBy("cnt_voice_internal_local")), lit(0)))
    .drop("cnt_voice_internal_local");

  val res = dmSubsSoc
    .join(voiceDataFillUp, Seq("ctn", "ban"), "left")
    .join(usageFull, Seq("ctn", "ban"), "left")
    .join(ppParam
      .join(trParamData, Seq("market_key", "count_gprs"))
      .join(trParamVoice, Seq("market_key", "soc_voice")),
      Seq("market_key"))
    .filter(col("voice_usage_pred").between(col("prev_cnt_voice"), col("cnt_voice_internal_local")) &&
      col("data_usage_pred").between(col("prev_gprs"), col("count_gprs")))
    .withColumn("rn", row_number.over(Window.partitionBy("ctn", "ban")
      .orderBy(rand())))
    .filter(col("rn") === 1)
    .drop("rn")
    .join(base, Seq("ctn", "ban"), "left")
    .withColumn("current_minutes", when(col("current_minutes").isNull, col("curr_voice_fill")).otherwise(col("current_minutes")))
    .withColumn("current_gb", when(col("current_gb").isNull, col("curr_data_fill")).otherwise(col("current_gb")))
    .withColumn("time_key", lit(SCORE_DATE))
    .select(
      col("ctn"),
      col("ban"),
      col("base"),
      dmSubsSoc("market_key"),
      col("org_soc_code"),
      col("business_name"),
      col("monthly_fee"),
      col("daily_fee"),
      col("current_minutes"),
      col("current_gb"),
      col("count_sms"),
      col("voice_usage"),
      col("data_usage"),
      col("data_usage_pred"),
      col("voice_usage_pred"),
      col("price_plan"),
      col("constructor_id"),
      col("constructor_daily_fee"),
      col("constructor_monthly_fee"),
      col("cnt_voice_internal_local").as("constructor_minutes"),
      col("count_gprs").as("constructor_gb"),
      col("time_key")
    );

}
