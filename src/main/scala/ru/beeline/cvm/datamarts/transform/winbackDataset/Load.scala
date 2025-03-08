package ru.beeline.cvm.datamarts.transform.winbackDataset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class Load extends CustomParams {

  private val lastSubsWindow = Window.partitionBy("subs_key", "ban_key").orderBy("first_time_key")
  private val nbaCustomersPub = spark.read.table(TableNbaCustomersPub)
    .filter(col("time_key") === SCORE_DATE)
    .filter(col("curr_subs_status_key") === "S")
    .filter(col("account_type_key") === "13")
    .select(
      col("last_ctn").as("subs_key"),
      col("last_ban").as("ban_key")
    )

  def columnsProcessing[T](featureName: String,
                           aggFun: String,
                           nullReplaceValue: T,
                           tableName: String,
                           min_lag: Int,
                           max_lag: Option[Int]): Column = {

    val firstTimeKey = substring(col("first_time_key"), 1, 7)

    val monthsBetween = tableName match {
      case "fct_act" => floor(
        months_between(firstTimeKey,
          concat_ws("-", substring(col("time_key"), 2, 4),
            substring(col("time_key"), 6, 2)
          )))
      case _ => floor(
        months_between(firstTimeKey, substring(col("time_key"), 1, 7)))
    }

    val (whenCondition, alias) = max_lag match {
      case Some(lag) if (tableName == "usage") => (
        monthsBetween === 1 &&
          dayofmonth(col("time_key")).between(PREV_MONTH_FEAT_START_DAY, PREV_MONTH_FEAT_END_DAY),
        s"${featureName}_${aggFun}_lag_${min_lag}_${lag}")
      case Some(lag) => (
        monthsBetween.between(min_lag, lag),
        s"${featureName}_${aggFun}_lag_${min_lag}_${lag}")
      case None => (monthsBetween === min_lag, s"${featureName}_${aggFun}_lag_${min_lag}")
    }

    val column = when(whenCondition, col(featureName)).otherwise(null)

    val columnAfterApplyFun = aggFun match {
      case "mean" => mean(column)
      case "stddev_samp" => stddev_samp(column)
      case "max" => max(column)
    }

    when(columnAfterApplyFun.isNull, lit(nullReplaceValue))
      .otherwise(columnAfterApplyFun)
      .as(alias)
  }

  def getUsageCtn(): DataFrame = {

    val meanCol = FEATURES_AGG_CTN_USAGE_WITH_LAGS.flatMap {
      case (key, value) => Seq(
        columnsProcessing(key, "mean", FEATURES_AGG_CTN_USAGE_NULL_REPLACE(key), "usage", value._1, Some(value._2)))
    }.toSeq

    spark.read.table(TableAggCtnUsagePubD)
      .filter(col("time_key").between(LOAD_DATE_YYYYY_MM_01_1M, LOAD_DATE_YYYYY_MM_14_1M))
      .select(
        col("ctn").as("subs_key"),
        col("ban").as("ban_key"),
        lit(SCORE_DATE_12).as("first_time_key"),
        col("internet_active_flg"),
        col("voice_out_active_flg"),
        col("voice_input_active_flg"),
        col("time_key"))
      .join(nbaCustomersPub, Seq("subs_key", "ban_key"), "left")
      .groupBy(
        col("subs_key"),
        col("ban_key"),
        col("first_time_key"))
      .agg(meanCol.head, meanCol.tail: _*)
      .withColumn("rn", row_number().over(lastSubsWindow))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  def getSms(): DataFrame = {

    val aggColumn = FEATURES_AGG_CTN_SMS_CAT_WITH_LAGS.flatMap {
      case (key, value) => Seq(
        columnsProcessing(key, "mean", FEATURES_AGG_CTN_SMS_CAT_NULL_REPLACE(key), "sms", value._1, Some(value._2)),
        columnsProcessing(key, "stddev_samp", FEATURES_AGG_CTN_SMS_CAT_NULL_REPLACE(key), "sms", value._1, Some(value._2)))
    }.toSeq

    spark.read.table(TableAggCtnSmsCatPubM)
      .filter(col("time_key").between(LOAD_DATE_YYYY_MM_DD_4M, LOAD_DATE_YYYY_MM_LAST_DAY_2M))
      .select(
        col("ctn").as("subs_key"),
        col("ban").as("ban_key"),
        (col("sms_bank_cnt_m") / dayofmonth(last_day(col("time_key")))).as("normalized_sms_bank_cnt_m"),
        lit(SCORE_DATE_12).as("first_time_key"),
        col("time_key"))
      .join(nbaCustomersPub, Seq("subs_key", "ban_key"), "left")
      .groupBy(
        col("subs_key"),
        col("ban_key"),
        col("first_time_key"))
      .agg(aggColumn.head, aggColumn.tail: _*)
      .withColumn("rn", row_number().over(lastSubsWindow))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  def getCtnBalance(): DataFrame = {

    val aggColumn = FEATURES_AGG_CTN_BALANCE_WITH_LAGS.flatMap {
      case (key, value) => Seq(
        columnsProcessing(key, "mean", FEATURES_AGG_CTN_BALANCE_NULL_REPLACES(key), "balance", value._1, Some(value._2)),
        columnsProcessing(key, "stddev_samp", FEATURES_AGG_CTN_BALANCE_NULL_REPLACES(key), "balance", value._1, Some(value._2)))
    }.toSeq

    spark.read.table(TableAggCtnBalancePubM)
      .filter(col("time_key").between(LOAD_DATE_YYYY_MM_DD_4M, LOAD_DATE_YYYY_MM_LAST_DAY_2M))
      .select(
        col("ctn").as("subs_key"),
        col("ban").as("ban_key"),
        (col("bal_neg_cnt_m") / dayofmonth(last_day(col("time_key")))).as("normalized_bal_neg_cnt_m"),
        lit(SCORE_DATE_12).as("first_time_key"),
        col("time_key"))
      .join(nbaCustomersPub, Seq("subs_key", "ban_key"), "left")
      .groupBy(
        col("subs_key"),
        col("ban_key"),
        col("first_time_key"))
      .agg(aggColumn.head, aggColumn.tail: _*)
      .withColumn("rn", row_number().over(lastSubsWindow))
      .filter(col("rn") === 1)
      .drop("rn")
  }

  def getFctActClust(): DataFrame = {
    val cols = Seq("total_recharge_amt_rur", "cnt_recharge", "total_rev", "direct_rev", "total_contact",
      "cnt_voice_day", "cnt_data_day", "mou_out_bln", "device_type", "bs_cnt_voice", "bs_cnt_data")

    val normalizeCols = cols.map {
      case colName@"device_type" => when(col(colName).isin("A", "S"), lit(1))
        .otherwise(lit(0)).as("is_smartphone")

      case colName@("bs_cnt_voice" | "bs_cnt_data") => when(col(colName) > 0,
        when((col(colName) / col("bs_cnt_total")).isNull, lit(0))
          .otherwise(col(colName) / col("bs_cnt_total")))
        .otherwise(lit(0)).as(s"bs_${colName.substring(7)}_rate")

      case colName@_ => (col(colName) /
        dayofmonth(last_day(
          date_format(to_date(substring(col("time_key"), 2, 6), "yyyyMM"), "yyyy-MM-01"))))
        .as(s"normalized_$colName")
    } ++ Seq(col("last_subs_key").as("subs_key"),
      col("time_key"), col("last_ban_key").as("ban_key"),
      col("market_key_src").as("market_key"), col("cluster_num"), col("permanent_aab"))

    val clust = spark.table(TableFctActClustSubsMPub)
      .filter(col("time_key").between(LOAD_DATE_PYYYYMM_4M, LOAD_DATE_PYYYYMM_2M))
      .select(normalizeCols: _*)
      .join(nbaCustomersPub, Seq("subs_key", "ban_key"), "left")
      .withColumn("first_time_key", lit(SCORE_DATE_12))

    val feature1 = FEATURES_FCT_ACT_CLUST_SUBS_M_WITH_LAGS.flatMap {
      case (key, value) =>
        Seq(columnsProcessing(
          key, "mean",
          FEATURES_FCT_ACT_CLUST_SUBS_M_NULL_REPLACES(key),
          "fct_act", value._1, Some(value._2)),
        columnsProcessing(
          key, "stddev_samp",
          FEATURES_FCT_ACT_CLUST_SUBS_M_NULL_REPLACES(key),
          "fct_act", value._1, Some(value._2)))
    }.toSeq

    val feature2 = FEATURES_WOUT_AGG_WITH_LAGS.flatMap {
      case (key, value) => Seq(columnsProcessing(key, "max",
        FEATURES_FCT_ACT_CLUST_SUBS_M_NULL_REPLACES(key), "fct_act", value._1, value._2))
    }.toSeq

    val allFeature = feature1 ++ feature2
    clust.groupBy(col("subs_key"), col("ban_key"), col("market_key"))
      .agg(allFeature.head, allFeature.tail: _*)
  }
}
