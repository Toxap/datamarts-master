package ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD.staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class AggCtnTrafficConstructD extends Load {


  val dimSubscriberPub = getDimSubscriber
  val fctUsagePrepChaNPub = getFctUsagePrepChaN
  val fctUsagePrepOgprsNPub = getFctUsagePrepOgprsN

  val voiceTraf = getTrafDF(dimSubscriberPub, fctUsagePrepChaNPub, "V")
  val dataTraf = getTrafDF(dimSubscriberPub, fctUsagePrepOgprsNPub, "D")

  val res = getFinalTrafDF(dataTraf, voiceTraf)

  def getTrafDF(dimSubscriberDF: DataFrame, trafDF: DataFrame, typeData: String): DataFrame = {
    dimSubscriberDF
      .join(trafDF, Seq("ctn", "ban"), "left")
      .groupBy(dimSubscriberDF.col("ctn"),
        dimSubscriberDF.col("ban"),
        dimSubscriberDF.col("market"),
        col("price_plan_change_date"),
        col("full_week_num"),
        col("end_of_period"))
      .agg(sum(when(col("call_start_time_src")
        .between(when(col("full_week_num") > 3,
          date_add(col("end_of_period"), -20))
          .otherwise(col("price_plan_change_date")),
          col("end_of_period")),
        when(lit(typeData) === "V", col("actual_call_duration_sec"))
          .otherwise(col("rounded_data_volume")))
        .otherwise(0))
        .as("traf_value"))
      .withColumn("data_type", lit(typeData))
  }

  def getFinalTrafDF(dataDF: DataFrame, voiceDF: DataFrame): DataFrame = {
    dataDF.union(voiceDF)
      .withColumn("voice_traf", when(col("data_type") === "V", col("traf_value")).otherwise(lit(0)))
      .withColumn("data_traf", when(col("data_type") === "D", col("traf_value")).otherwise(lit(0)))
      .groupBy(col("ctn"),
        col("ban"),
        col("market"),
        col("price_plan_change_date"),
        col("full_week_num"))
      .agg(
        round(sum(col("data_traf")) / 1024 / when(col("full_week_num") > 3, 3)
          .otherwise(col("full_week_num")), 2).as("avg_data_traffic"),
        round(sum(col("voice_traf")) / when(col("full_week_num") > 3, 3)
          .otherwise(col("full_week_num")), 2).as("avg_voice_traffic")
      )
      .withColumn("report_dt", lit(current_date()))
      .withColumn("time_key", lit(LOAD_DATE_YYYY_MM_DD_4))
  }

  def persistSources: Unit = dimSubscriberPub.persist()

  def unpersistSources: Unit = dimSubscriberPub.unpersist()
}
