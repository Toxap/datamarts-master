package ru.beeline.cvm.datamarts.transform.subPresetRoaming

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.DataFrame

class subPresetRoaming extends Load{

  val beemetrics: DataFrame = readMobileSubscriberUsageDaily

  private val ctnBanTimeKeyMemTable: DataFrame = readFilteredMobileSubscriber

  private val usageDailyRoaming = beemetrics
    .filter(col("roaming_cd") === lit("I"))
    .filter(col("activity_cd").isin("VOICE", "SMS", "GPRS"))
    .filter(col("transaction_dt") >= SCORE_DATE_MINUS_ONE_YEAR);

  private val roamingActivity =
    ctnBanTimeKeyMemTable
      .join(usageDailyRoaming,
        Seq("subscriber_num", "ban_num"),
        "left"
      )
      .filter(col("usage_amt") > 0)
      .select(
        ctnBanTimeKeyMemTable.col("*"),
        usageDailyRoaming.col("transaction_dt"),
        usageDailyRoaming.col("activity_cd"),
        usageDailyRoaming.col("call_direction_cd"),
        usageDailyRoaming.col("usage_amt")
      );

  roamingActivity.createOrReplaceTempView("roamingActivity");

  private val tripDuration = roamingActivity
    .groupBy("subscriber_num", "ban_num")
    .agg((datediff(max("transaction_dt"), min("transaction_dt")) + 1).as("duration"))
    .select(
      col("subscriber_num"),
      col("ban_num"),
      col("duration")
    );

  private val flagJoinedMemTcamps = roamingActivity
    .withColumn("flag_voice",
      when(col("activity_cd") === "VOICE", 1)
        .otherwise(0))
    .withColumn("flag_voice_in",
      when((col("activity_cd") === "VOICE") && (col("call_direction_cd") === 1), 1)
        .otherwise(0))
    .withColumn("flag_voice_out",
      when((col("activity_cd") === "VOICE") && (col("call_direction_cd") === 2), 1)
        .otherwise(0))
    .withColumn("flag_data",
      when(col("activity_cd") === "GPRS", 1)
        .otherwise(0))
    .withColumn("flag_sms_out",
      when((col("activity_cd") === "SMS") && (col("call_direction_cd") === 2), 1)
        .otherwise(0));


  private val aggFlagJoinedMemTcamps = flagJoinedMemTcamps
    .groupBy("subscriber_num", "ban_num")
    .agg(
      sum(col("flag_voice") * col("usage_amt")).alias("voice_amt"),
      sum(col("flag_voice_in") * col("usage_amt")).alias("voice_in_amt"),
      sum(col("flag_voice_out") * col("usage_amt")).alias("voice_out_amt"),
      sum(col("flag_data") * col("usage_amt")).alias("data_amt"),
      sum(col("flag_sms_out") * col("usage_amt")).alias("sms_out_amt")
    );

  private val aggVoiceDataFlagJoinedMemTcamps = aggFlagJoinedMemTcamps
    .withColumn("voice_amt", col("voice_amt") / 60)
    .withColumn("voice_in_amt", col("voice_in_amt") / 60)
    .withColumn("voice_out_amt", col("voice_out_amt") / 60)
    .withColumn("data_amt", col("data_amt") / 1024 / 1024 / 1024);

  private val filteredVoiceDataJoinedMemTcamps = flagJoinedMemTcamps
    .filter(col("flag_voice") === 1)
    .groupBy("subscriber_num", "ban_num")
    .agg(countDistinct("transaction_dt").as("voice_days"));

  private val voiceInJoinedMemTcamps = flagJoinedMemTcamps
    .filter(col("flag_voice_in") === 1)
    .groupBy("subscriber_num", "ban_num")
    .agg(countDistinct("transaction_dt").as("voice_in_days"));

  private val voiceOutJoinedMemTcamps = flagJoinedMemTcamps
    .filter(col("flag_voice_out") === 1)
    .groupBy("subscriber_num", "ban_num")
    .agg(countDistinct("transaction_dt").as("voice_out_days"));

  private val dataJoinedMemTcamps = flagJoinedMemTcamps
    .filter(col("flag_data") === 1)
    .groupBy("subscriber_num", "ban_num")
    .agg(countDistinct("transaction_dt").as("data_days"));

  private val smsJoinedMemTcamps = flagJoinedMemTcamps
    .filter(col("flag_sms_out") === 1)
    .groupBy("subscriber_num", "ban_num")
    .agg(countDistinct("transaction_dt").as("sms_days"));

  private val joinAllFeatures = aggVoiceDataFlagJoinedMemTcamps
    .join(filteredVoiceDataJoinedMemTcamps, Seq("subscriber_num", "ban_num"), "left")
    .join(voiceInJoinedMemTcamps, Seq("subscriber_num", "ban_num"), "left")
    .join(voiceOutJoinedMemTcamps, Seq("subscriber_num", "ban_num"), "left")
    .join(dataJoinedMemTcamps, Seq("subscriber_num", "ban_num"), "left")
    .join(smsJoinedMemTcamps, Seq("subscriber_num", "ban_num"), "left");

  private val dfWithNeedCols = joinAllFeatures
    .select(
      col("subscriber_num"),
      col("ban_num"),
      col("voice_amt"),
      col("voice_days"),
      col("voice_in_amt"),
      col("voice_in_days"),
      col("voice_out_amt"),
      col("voice_out_days"),
      col("data_amt"),
      col("data_days"),
      col("sms_out_amt"),
      col("sms_days")
    )
    .na.fill(0, Seq("voice_amt", "voice_days", "voice_in_amt", "voice_in_days",
      "voice_out_amt", "voice_out_days", "data_amt", "data_days", "sms_out_amt", "sms_days"));

  private val usageWithTripDurationTypeUsers: DataFrame = dfWithNeedCols
    .join(tripDuration, Seq("subscriber_num", "ban_num"), "left")
    .withColumn("type_of_users",
      when(
        col("voice_days") > 0 &&
          col("data_days") === 0 &&
          col("sms_days") === 0,
        lit("voice"))
        .when(
          col("data_days") > 0 &&
            col("voice_days") === 0 &&
            col("sms_days") ===0,
          lit("data"))
        .when(
          col("data_days") === 0 &&
            col("voice_days") === 0 &&
            col("sms_days") > 0,
          lit("sms"))
        .when(
          col("data_days") === 0 &&
            col("voice_days") === 0 &&
            col("sms_days") === 0,
          lit("inactive")
        ).otherwise(lit("loading"))
    );

  private val usageWithDurationNewType = usageWithTripDurationTypeUsers
    .withColumn("duration_new",
      when(
        col("duration") === 1,
        1)
        .when(
          col("duration") > 1 &&
            col("duration") <= 3,
          3
        )
        .when(
          col("voice_days") <= 1 &&
            col("data_days") <= 1 &&
            col("sms_days") <= 1 &&
            col("type_of_users") =!= "inactive",
          1
        )
        .otherwise(99)
    );

  val windowSpec: WindowSpec = Window.partitionBy("subscriber_num", "ban_num").orderBy("transaction_dt");

  private val testRoaming =
    spark.sql("SELECT subscriber_num, ban_num, transaction_dt" +
      " FROM roamingActivity" +
      " WHERE usage_amt>0" +
      " GROUP BY subscriber_num, ban_num, transaction_dt");

  private val step2testData =
    testRoaming
      .withColumn("next_1", lead("transaction_dt", 1).over(windowSpec))
      .withColumn("next_days_count", datediff(lead("transaction_dt", 1).over(windowSpec), col("transaction_dt")));

  private val step3testData =
    step2testData
      .withColumn("ggg", count(when(col("next_days_count") =!= 1 || col("next_days_count").isNull, 1)).over(windowSpec));

  private val windowSpec1 = Window.partitionBy("subscriber_num", "ban_num", "ggg").orderBy("transaction_dt");

  private val subQuery =
    step3testData
      .withColumn("D", row_number().over(windowSpec1));

  private val finalQuery =
    subQuery
      .groupBy("subscriber_num", "ban_num")
      .agg(max("D").alias("max_D"))
      .select(
        col("subscriber_num"),
        col("ban_num"),
        col("max_D")
      );

  private val durationJoinedTest =
    usageWithDurationNewType
      .join(finalQuery, Seq("subscriber_num", "ban_num"), "left")
      .withColumn("duration_new_max_trip",
        when(col("duration_new") === 99 && col("type_of_users") =!= "inactive",
          when(col("max_D") === 1, lit(1))
            .when(col("max_D") > 1 && col("max_D") <= 3, lit(3))
            .when(col("max_D") > 3 && col("max_D") <= 7, lit(7))
            .when(col("max_D") > 7 && col("max_D") <= 30, lit(30))
            .otherwise(lit(30))
        )
          .otherwise(col("duration_new"))
      );

  private val dfWithAverages: DataFrame = durationJoinedTest
    .withColumn("voice_avg", col("voice_amt").cast("double") / col("voice_days"))
    .withColumn("sms_avg", col("sms_out_amt").cast("double") / col("sms_days"))
    .withColumn("data_avg", col("data_amt").cast("double") / col("data_days"))
    .drop("duration");

  val schema: StructType = StructType(Array(
    StructField("duration", IntegerType, nullable = true),
    StructField("Data", FloatType, nullable = true),
    StructField("Voice", IntegerType, nullable = true),
    StructField("sms", IntegerType, nullable = true)
  ));

  val data: Seq[(Integer, Float, Integer, Integer)] = Seq(
    (1, 0.5f, 5, null.asInstanceOf[Integer]),
    (3, 1.0f, 10, 10),
    (7, 3.0f, 30, null.asInstanceOf[Integer]),
    (30, 5.0f, 60, null.asInstanceOf[Integer])
  );

  val socPackageRoaming: DataFrame = spark.createDataFrame(data).toDF("duration", "Data", "Voice", "sms")
    .select(
      col("duration").cast("float"),
      col("Data").cast("float"),
      col("Voice").cast("float"),
      col("sms").cast("float")
    );

  private val joinedAvgDataVoiceSocPackageRoaming: DataFrame =
    dfWithAverages
      .join(socPackageRoaming, dfWithAverages("duration_new_max_trip")===socPackageRoaming("duration"), "left")
      .withColumn("voice_pack_usage", col("voice_avg") * col("duration_new_max_trip") / col("Voice"))
      .withColumn("data_pack_usage", col("data_avg") * col("duration_new_max_trip") / col("Data"))
      .withColumn("sms_pack_usage",
        when(col("duration_new_max_trip") <= 3,
          col("sms_out_amt") / 10)
          .otherwise(col("sms_avg") * 3/10))
      .na.fill(0,Seq("voice_pack_usage", "data_pack_usage", "sms_pack_usage"));

  val typeUsersAvgDuration1: DataFrame = joinedAvgDataVoiceSocPackageRoaming
    .withColumn("type_of_users_avg",
      when(col("type_of_users") === "loading",
        when((col("voice_pack_usage") > 0) &&
          (col("voice_pack_usage") > col("data_pack_usage")) &&
          (col("voice_pack_usage") > col("sms_pack_usage")),
          lit("voice"))
          .when((col("data_pack_usage") > 0) &&
            (col("data_pack_usage") > col("voice_pack_usage")) &&
            (col("data_pack_usage") > col("sms_pack_usage")),
            lit("data"))
          .when((col("sms_pack_usage") > 0) &&
            (col("sms_pack_usage") > col("voice_pack_usage")) &&
            (col("sms_pack_usage") > col("data_pack_usage")),
            lit("sms"))
          .otherwise(lit("loading"))
      ).otherwise(col("type_of_users"))
    );

  private val typeUsersAvgDuration2 = typeUsersAvgDuration1
    .withColumn("duration_final",
      when(col("type_of_users_avg") =!= "loading",
        when((col("type_of_users_avg") === "voice") &&
          (col("voice_pack_usage") <= 1), col("duration_new_max_trip"))
          .when((col("type_of_users_avg") === "data") &&
            (col("data_pack_usage") <= 1), col("duration_new_max_trip"))
          .when(col("type_of_users_avg") === "sms",
            lit(3))
          .otherwise(lit(99))
      ).otherwise(lit(99))
    )
    .withColumn("duration_final",
      when(col("duration_final") === 99 &&
        col("duration_new_max_trip") === 7,
        lit(30))
        .otherwise(col("duration_final"))
    );
  private val typeUsersAvgDuration3 = typeUsersAvgDuration2
    .withColumn("duration_final",
      when((col("duration_final") === 99) &&
        (col("type_of_users_avg") === "voice"),
        when(col("voice_avg") * col("duration_new_max_trip") / 10 <= 1,
          lit(3))
          .when(col("voice_avg") * col("duration_new_max_trip") / 30 <= 1,
            lit(7))
          .otherwise(lit(30))
      ).otherwise(col("duration_final"))
    );

  private val typeUsersAvgDuration4 = typeUsersAvgDuration3
    .withColumn("duration_final",
      when((col("duration_final") === 99) &&
        (col("type_of_users_avg") === "data"),
        when(col("data_avg") * col("duration_new_max_trip") / 1 <= 1,
          lit(3))
          .when(col("data_avg") * col("duration_new_max_trip") / 3 <= 1,
            lit(7))
          .otherwise(lit(30))
      ).otherwise(col("duration_final"))
    );

  val socsData: Seq[(String, String)] = Seq(
    ("data1", "ROAM500MB"),
    ("data3", "ROAM1GB"),
    ("data7", "ROAM3GB"),
    ("data30", "ROAMG5GB"),
    ("voice1", "ROAM5MIN"),
    ("voice3", "ROAM10MIN"),
    ("voice7", "ROAM30MIN"),
    ("voice30", "ROAM60MIN"),
    ("sms3", "ROAM10SMS")
  );

  import spark.implicits._

  val dfSocs: DataFrame = socsData.toDF("key", "soc")

  val dfMerged: DataFrame = typeUsersAvgDuration4
    .withColumn("joinKey", concat(col("type_of_users_avg"), col("duration_final").cast("string")))
    .join(dfSocs, col("joinKey") === col("key"), "left")
    .filter(col("soc").isNotNull)
    .withColumn("report_dt",lit(LOAD_DATE_YYYY_MM_DD))
    .select(
      "subscriber_num",
      "ban_num",
      "voice_amt",
      "voice_days",
      "data_amt",
      "data_days",
      "sms_out_amt",
      "sms_days",
      "max_D",
      "duration_new_max_trip",
      "voice_pack_usage",
      "data_pack_usage",
      "sms_pack_usage",
      "type_of_users_avg",
      "duration_final",
      "soc",
      "report_dt"
    )
}
