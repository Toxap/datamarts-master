package ru.beeline.cvm.datamarts.transform.commPolit

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

trait addFunction extends CustomParams {

  val in_voice_cols: List[String] = List(
    "in_voice_fix_local", "in_voice_fix_mg",
    "in_voice_fix_mn", "in_voice_h_local",
    "in_voice_h_mg", "in_voice_h_mn",
    "in_voice_int", "in_voice_offnet_local_mgf",
    "in_voice_offnet_local_mts", "in_voice_offnet_local_other",
    "in_voice_offnet_local_tl2", "in_voice_offnet_mg",
    "in_voice_offnet_mn", "in_voice_r_local",
    "in_voice_r_mg", "in_voice_r_mn");
  val out_voice_cols: List[String] = List(
    "out_voice_fix_local", "out_voice_fix_mg",
    "out_voice_fix_mn", "out_voice_h_local",
    "out_voice_h_mg", "out_voice_h_mn",
    "out_voice_int", "out_voice_offnet_local_mgf",
    "out_voice_offnet_local_mts", "out_voice_offnet_local_other",
    "out_voice_offnet_local_tl2", "out_voice_offnet_mg",
    "out_voice_offnet_mn", "out_voice_r_local",
    "out_voice_r_mg", "out_voice_r_mn");

  val in_voice: Column = in_voice_cols.map(col).reduce(_ + _);
  val out_voice: Column = out_voice_cols.map(col).reduce(_ + _);

  val fctFeaturesWithTypes: Map[String, NumericType] = Map(
    "bs_data_rate" -> FloatType,
    "bs_voice_rate" -> FloatType,
    "total_recharge_amt_rur" -> FloatType,
    "cnt_recharge" -> IntegerType,
    "total_contact" -> IntegerType,
    "permanent_aab" -> IntegerType,
    "cnt_voice_day" -> ByteType,
    "cnt_data_day" -> ByteType,
    "is_smartphone" -> ByteType,
    "days_in_status_c" -> IntegerType,
    "direct_rev" -> FloatType,
    "total_rev" -> FloatType,
    "data_mb" -> FloatType,
    "in_voice" -> IntegerType,
    "out_voice" -> IntegerType,
    "pp_day" -> IntegerType
  );

  val featAggLagsCols: Map[String, List[Int]] = Map(
    "normalized_bs_data_rate" -> List(2, 4),
    "normalized_bs_voice_rate" -> List(2, 4),
    "normalized_total_recharge_amt_rur" -> List(2, 4),
    "normalized_cnt_recharge" -> List(2, 4),
    "normalized_total_contact" -> List(2, 4),
    "normalized_cnt_voice_day" -> List(2, 4),
    "normalized_cnt_data_day" -> List(2, 4),
    "normalized_direct_rev" -> List(2, 4),
    "normalized_total_rev" -> List(2, 4),
    "normalized_data_mb" -> List(2, 4),
    "normalized_in_voice" -> List(2, 4),
    "normalized_out_voice" -> List(2, 4)
  );

  val featWoutAgg: Map[String, List[Int]] = Map(
    "permanent_aab" -> List(2),
    "is_smartphone" -> List(2),
    "days_in_status_c" -> List(2),
    "pp_day" -> List(2)
  );

  val nullReplaces: Map[String, Int] = Map(
    "normalized_bs_data_rate" -> 0,
    "normalized_bs_voice_rate" -> 0,
    "normalized_total_recharge_amt_rur" -> 0,
    "normalized_cnt_recharge" -> 0,
    "normalized_total_contact" -> 0,
    "permanent_aab" -> 0,
    "normalized_cnt_voice_day" -> 0,
    "normalized_cnt_data_day" -> 0,
    "is_smartphone" -> 0,
    "days_in_status_c" -> -999999,
    "normalized_direct_rev" -> 0,
    "normalized_total_rev" -> 0,
    "normalized_data_mb" -> 0,
    "normalized_in_voice" -> 0,
    "normalized_out_voice" -> 0,
    "pp_day" -> -1
  );

  val daysInMonthCol: Column = dayofmonth(
    last_day(
      concat_ws(
        "-",
        substring(col("clust_time_key"), 2, 4),
        substring(col("clust_time_key"), 6, 2),
        lit("01")
      ).cast(DateType)
    )
  );
  val colsToNormalize: List[String] = List(
    "bs_data_rate",
    "bs_voice_rate",
    "total_recharge_amt_rur",
    "cnt_recharge",
    "total_contact",
    "cnt_voice_day",
    "cnt_data_day",
    "direct_rev",
    "total_rev",
    "data_mb",
    "in_voice",
    "out_voice"
  );

  def convertColumnTypes(df: DataFrame, fctFeaturesWithTypes: Map[String, NumericType]): DataFrame = {
    fctFeaturesWithTypes.keys.foldLeft(df) { (accDF, f) =>
      val t = fctFeaturesWithTypes(f)
      accDF.withColumn(f, col(f).cast(t))
    }
  }

  // Функция для нормализации столбцов
  def normalizeColumns(df: DataFrame, colsToNormalize: Seq[String], daysInMonthCol: Column): DataFrame = {
    colsToNormalize.foldLeft(df) { (accDF, f) =>
      accDF.withColumn(s"normalized_$f", col(f) / daysInMonthCol)
    }
  }

  def createMeanAggColumns(featAggLagsCols: Map[String, List[Int]], nullReplaces: Map[String, Int]): ArrayBuffer[Column] = {
    val feat_lag_agg_cols = scala.collection.mutable.ArrayBuffer[Column]()

    for ((feature_name, lags) <- featAggLagsCols) {
      val mean_col_val = mean(
        when(
          floor(months_between(
            substring(col("b.time_key_src"), 1, 7),
            concat_ws(
              "-",
              substring(col("clust_time_key"), 2, 4),
              substring(col("clust_time_key"), 6, 2)
            )
          )).between(lags(0), lags(1)),
          col(feature_name)
        )
          .otherwise(null)
      )

      val mean_agg_col = when(
        mean_col_val.isNull,
        lit(nullReplaces(feature_name))
      ).otherwise(mean_col_val)
        .alias(s"${feature_name}_mean_lag${lags(0)}_${lags(1)}")

      feat_lag_agg_cols += mean_agg_col
    }
    feat_lag_agg_cols
  }

  def createStdAggColumns(featAggLagsCols: Map[String, List[Int]], nullReplaces: Map[String, Int]): ArrayBuffer[Column] = {
    val feat_lag_agg_cols = scala.collection.mutable.ArrayBuffer[Column]()

    for ((feature_name, lags) <- featAggLagsCols) {
      val std_col_val = stddev_samp(
        when(
          floor(months_between(
            substring(col("b.time_key_src"), 1, 7),
            concat_ws(
              "-",
              substring(col("clust_time_key"), 2, 4),
              substring(col("clust_time_key"), 6, 2)
            )
          )).between(lags(0), lags(1)),
          col(feature_name)
        )
          .otherwise(null)
      )

      val std_agg_col = when(std_col_val.isNull,
        lit(nullReplaces(feature_name)))
        .when(std_col_val.isNaN, lit(nullReplaces(feature_name)))
        .otherwise(std_col_val)
        .alias(s"${feature_name}_std_lag${lags(0)}_${lags(1)}")

      feat_lag_agg_cols += std_agg_col
    }
    feat_lag_agg_cols
  }

  def createColumnsWithoutAggregation(featWoutAgg: Map[String, List[Int]],
                                      nullReplaces: Map[String, Int]): ArrayBuffer[Column] = {
    val feat_lag_wout_agg_cols = scala.collection.mutable.ArrayBuffer[Column]()

    for ((feature_name, lags) <- featWoutAgg) {
      for (lag <- lags) {
        val col_val = max(
          when(
            floor(months_between(
              substring(col("b.time_key_src"), 1, 7),
              concat_ws(
                "-",
                substring(col("clust_time_key"), 2, 4),
                substring(col("clust_time_key"), 6, 2)
              )
            )) === lag,
            col(feature_name)
          )
            .otherwise(null)
        )

        val cols = when(
          col_val.isNull,
          lit(nullReplaces(feature_name))
        ).otherwise(col_val)
          .alias(s"${feature_name}_lag$lag")

        feat_lag_wout_agg_cols += cols
      }
    }
    feat_lag_wout_agg_cols
  }


}
