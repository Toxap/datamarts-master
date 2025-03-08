package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}

trait addFunctions extends CustomParams {

  val marksColumnsData = Seq(col("_1_r_data_usage_1_6"), col("_2_r_data_usage_1_6"),
    col("_3_r_data_usage_1_6"), col("_4_r_data_usage_1_6"), col("_5_r_data_usage_1_6"),
    col("_6_r_data_usage_1_6"))
  val marksColumnsVoice = Seq(col("_1_r_voice_usage_1_6"), col("_2_r_voice_usage_1_6"),
    col("_3_r_voice_usage_1_6"), col("_4_r_voice_usage_1_6"), col("_5_r_voice_usage_1_6"),
    col("_6_r_voice_usage_1_6"))
  val marksColumnsArpu = Seq(col("_2_r_arpu_to_pp_2_7"), col("_3_r_arpu_to_pp_2_7"),
    col("_4_r_arpu_to_pp_2_7"), col("_5_r_arpu_to_pp_2_7"), col("_6_r_arpu_to_pp_2_7"),
    col("_7_r_arpu_to_pp_2_7"))

  def rowMean(cols: Seq[Column]): Column = {
    val sum = cols.map(coalesce(_, lit(0)))
      .foldLeft(lit(0))(_ + _)

    val cnt = cols.map(c => when(c.isNull, 0).otherwise(1))
      .foldLeft(lit(0))(_ + _)

    sum / cnt
  }

  def featureCollect(dataFrame: DataFrame,
                     lstFunction: Seq[String],
                     features: Seq[String],
                     groupColumn: Seq[String]): DataFrame = {

    val aggFeature = features.flatMap(x => lstFunction.map {
      case "min" => min(x).as(s"${x}_min")
      case "max" => max(x).as(s"${x}_max")
      case "mean" => mean(x).as(s"${x}_mean")
      case _ => throw new NoSuchElementException
    })
    dataFrame
      .groupBy(groupColumn.head, groupColumn.tail: _*)
      .agg(aggFeature.head, aggFeature.tail: _*)
  }

  def pivotsTable(dataFrame: DataFrame,
                  time_key: String,
                  features: Seq[String],
                  groupColumn: Seq[String],
                  oneFeature: Boolean = false): DataFrame = {
    val aggFeature = features.map(x => first(col(x)).as(x))
    val ptField = if (oneFeature)
    { concat_ws("_", lit(""),
      months_between(lit(FIRST_DAY_OF_MONTH_YYYY_MM_01), col(time_key)).cast(IntegerType), lit(features.head)) }
    else { concat_ws("_", lit(""),
      months_between(lit(FIRST_DAY_OF_MONTH_YYYY_MM_01), col(time_key)).cast(IntegerType)) }

    dataFrame
      .withColumn("tk", ptField)
      .groupBy(groupColumn.head, groupColumn.tail: _*)
      .pivot("tk")
      .agg(aggFeature.head, aggFeature.tail: _*)
  }
}
