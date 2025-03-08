package ru.beeline.cvm.datamarts.transform.productCategoryProfile

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
class ProductCategoryProfile extends Load {


  val threshold = thresholdProduct

  val tableModelScore = tablModelScore

  val dataframe = tableModelScore
    .join(threshold, Seq("product_id"), "left")
    .select(
      col("ctn"),
      col("product_id"),
      col("product"),
      threshold("use_case_impact_id"),
      col("pred_sale_calibrated_sms"),
      col("threshold_pred_sale_calibrated_sms"),
      col("pred_sale_calibrated_push"),
      col("threshold_pred_sale_calibrated_push"),
      col("pred_sale_calibrated_email"),
      col("threshold_pred_sale_calibrated_email"),
      col("pred_sale_calibrated_outbound"),
      col("threshold_pred_sale_calibrated_outbound"),
      col("pred_sale_calibrated_robot"),
      col("threshold_pred_sale_calibrated_robot"))
    .withColumn("avg_pred_sale_sms",
      avg(when(col("pred_sale_calibrated_sms") >= col("threshold_pred_sale_calibrated_sms"),
        col("pred_sale_calibrated_sms")).otherwise(null))
        .over(Window.partitionBy("product_id")))
    .withColumn("avg_pred_sale_push",
      avg(when(col("pred_sale_calibrated_push") >= col("threshold_pred_sale_calibrated_push"),
        col("pred_sale_calibrated_push")).otherwise(null))
        .over(Window.partitionBy("product_id")))
    .withColumn("avg_pred_sale_email",
      avg(when(col("pred_sale_calibrated_email") >= col("threshold_pred_sale_calibrated_email"),
        col("pred_sale_calibrated_email")).otherwise(null))
        .over(Window.partitionBy("product_id")))
    .withColumn("avg_pred_sale_outbound",
      avg(when(col("pred_sale_calibrated_outbound") >= col("threshold_pred_sale_calibrated_outbound"),
        col("pred_sale_calibrated_outbound")).otherwise(null))
        .over(Window.partitionBy("product_id")))
    .withColumn("avg_pred_sale_robot",
      avg(when(col("pred_sale_calibrated_robot") >= col("threshold_pred_sale_calibrated_robot"),
        col("pred_sale_calibrated_robot")).otherwise(null))
        .over(Window.partitionBy("product_id")));

  val updatedDataFrame = dataframe
    .withColumn("product_id",
      when(col("use_case_impact_id") === 1111213, lit(231))
        .when(col("use_case_impact_id") === 1111214, lit(232))
        .when(col("use_case_impact_id") === 1111215, lit(233))
        .when(col("use_case_impact_id") === 1111160, lit(234))
        .otherwise(col("product_id")))

  val smsProducts = Seq(1, 3, 8, 6, 9, 11, 12, 13, 14, 15, 16, 18, 231, 232, 234);
  val pushProducts = Seq(15, 14, 13, 3, 9, 18, 4, 231, 232, 233, 234);
  val emailProducts = Seq(18, 15, 14, 231);
  val outboundProducts = Seq(2, 18);
  val robotProducts = Seq(2);

  val smsPivot = updatedDataFrame
    .filter(col("product_id").isin(smsProducts: _*))
    .withColumn("category",
      when(col("pred_sale_calibrated_sms") < col("threshold_pred_sale_calibrated_sms"), "D")
        .when(
          (col("pred_sale_calibrated_sms") < lit(0.85) * col("avg_pred_sale_sms")) &&
            (col("pred_sale_calibrated_sms") >= col("threshold_pred_sale_calibrated_sms")), "C")
        .when(
          (col("pred_sale_calibrated_sms") >= lit(0.85) * col("avg_pred_sale_sms")) &&
            (col("pred_sale_calibrated_sms") <= lit(1.15) * col("avg_pred_sale_sms")), "B")
        .when(col("pred_sale_calibrated_sms") > lit(1.15) * col("avg_pred_sale_sms"), "A"))
    .groupBy("ctn")
    .pivot("product_id")
    .agg(first("category"));


  val pushPivot = updatedDataFrame
    .filter(col("product_id").isin(pushProducts: _*))
    .withColumn("category",
      when(col("pred_sale_calibrated_push") < col("threshold_pred_sale_calibrated_push"), "D")
        .when(
          (col("pred_sale_calibrated_push") < lit(0.85) * col("avg_pred_sale_push")) &&
            (col("pred_sale_calibrated_push") >= col("threshold_pred_sale_calibrated_push")), "C")
        .when(
          (col("pred_sale_calibrated_push") >= lit(0.85) * col("avg_pred_sale_push")) &&
            (col("pred_sale_calibrated_push") <= lit(1.15) * col("avg_pred_sale_push")), "B")
        .when(col("pred_sale_calibrated_push") > lit(1.15) * col("avg_pred_sale_push"), "A"))
    .groupBy("ctn")
    .pivot("product_id")
    .agg(first("category"));


  val emailPivot = updatedDataFrame
    .filter(col("product_id").isin(emailProducts: _*))
    .withColumn("category",
      when(col("pred_sale_calibrated_email") < col("threshold_pred_sale_calibrated_email"), "D")
        .when(
          (col("pred_sale_calibrated_email") < lit(0.85) * col("avg_pred_sale_email")) &&
            (col("pred_sale_calibrated_email") >= col("threshold_pred_sale_calibrated_email")), "C")
        .when(
          (col("pred_sale_calibrated_email") >= lit(0.85) * col("avg_pred_sale_email")) &&
            (col("pred_sale_calibrated_email") <= lit(1.15) * col("avg_pred_sale_email")), "B")
        .when(col("pred_sale_calibrated_email") > lit(1.15) * col("avg_pred_sale_email"), "A"))
    .groupBy("ctn")
    .pivot("product_id")
    .agg(first("category"));


  val outboundPivot = updatedDataFrame
    .filter(col("product_id").isin(outboundProducts: _*))
    .withColumn("category",
      when(col("pred_sale_calibrated_outbound") < col("threshold_pred_sale_calibrated_outbound"), "D")
        .when(
          (col("pred_sale_calibrated_outbound") < lit(0.85) * col("avg_pred_sale_outbound")) &&
            (col("pred_sale_calibrated_outbound") >= col("threshold_pred_sale_calibrated_outbound")), "C")
        .when(
          (col("pred_sale_calibrated_outbound") >= lit(0.85) * col("avg_pred_sale_outbound")) &&
            (col("pred_sale_calibrated_outbound") <= lit(1.15) * col("avg_pred_sale_outbound")), "B")
        .when(col("pred_sale_calibrated_outbound") > lit(1.15) * col("avg_pred_sale_outbound"), "A"))
    .groupBy("ctn")
    .pivot("product_id")
    .agg(first("category"));


  val robotPivot = updatedDataFrame
    .filter(col("product_id").isin(robotProducts: _*))
    .withColumn("category",
      when(col("pred_sale_calibrated_robot") < col("threshold_pred_sale_calibrated_robot"), "D")
        .when(
          (col("pred_sale_calibrated_robot") < lit(0.85) * col("avg_pred_sale_robot")) &&
            (col("pred_sale_calibrated_robot") >= col("threshold_pred_sale_calibrated_robot")), "C")
        .when(
          (col("pred_sale_calibrated_robot") >= lit(0.85) * col("avg_pred_sale_robot")) &&
            (col("pred_sale_calibrated_robot") <= lit(1.15) * col("avg_pred_sale_robot")), "B")
        .when(col("pred_sale_calibrated_robot") > lit(1.15) * col("avg_pred_sale_robot"), "A"))
    .groupBy("ctn")
    .pivot("product_id")
    .agg(first("category"));

  val smsReName = smsPivot
    .withColumnRenamed("1", "relevant_segm_tve_sms")
    .withColumnRenamed("3", "relevant_segm_umcs_fun_sms")
    .withColumnRenamed("6", "relevant_segm_umcs_transfabroad_sms")
    .withColumnRenamed("8", "relevant_segm_umcs_appstore_sms")
    .withColumnRenamed("9", "relevant_segm_umcs_appgallery_sms")
    .withColumnRenamed("11", "relevant_segm_umcs_parking_sms")
    .withColumnRenamed("12", "relevant_segm_autopay_mp_sms")
    .withColumnRenamed("13", "relevant_segm_secretary_sms")
    .withColumnRenamed("14", "relevant_segm_books_sms")
    .withColumnRenamed("15", "relevant_segm_cloud_sms")
    .withColumnRenamed("16", "relevant_segm_privetplus_sms")
    .withColumnRenamed("18", "relevant_segm_common_balance_sms")
    .withColumnRenamed("231", "relevant_segm_gamification_beeline_games_sms")
    .withColumnRenamed("232", "relevant_segm_gamification_games_external_sms")
    .withColumnRenamed("234", "relevant_segm_gamification_retention_sms");

  val pushReName = pushPivot
    .withColumnRenamed("3", "relevant_segm_umcs_fun_push")
    .withColumnRenamed("4", "relevant_segm_umcs_transport_push")
    .withColumnRenamed("9", "relevant_segm_umcs_appgallery_push")
    .withColumnRenamed("13", "relevant_segm_secretary_push")
    .withColumnRenamed("14", "relevant_segm_books_push")
    .withColumnRenamed("15", "relevant_segm_cloud_push")
    .withColumnRenamed("18", "relevant_segm_common_balance_push")
    .withColumnRenamed("231", "relevant_segm_gamification_beeline_games_push")
    .withColumnRenamed("232", "relevant_segm_gamification_games_external_push")
    .withColumnRenamed("233", "relevant_segm_gamification_quizzes_external_push")
    .withColumnRenamed("234", "relevant_segm_gamification_retention_push");

  val emailReName = emailPivot
    .withColumnRenamed("14", "relevant_segm_books_email")
    .withColumnRenamed("15", "relevant_segm_cloud_email")
    .withColumnRenamed("18", "relevant_segm_common_balance_email")
    .withColumnRenamed("231", "relevant_segm_gamification_beeline_games_email");


  val outboundReName = outboundPivot
    .withColumnRenamed("2", "relevant_segm_fmc_outbound")
    .withColumnRenamed("18", "relevant_segm_common_balance_outbound");

  val robotReName = robotPivot
    .withColumnRenamed("2", "relevant_segm_fmc_robot");

  val decemberData = smsReName
    .join(pushReName, Seq("ctn"), "left")
    .join(emailReName, Seq("ctn"), "left")
    .join(outboundReName, Seq("ctn"), "left")
    .join(robotReName, Seq("ctn"), "left")
    .withColumn("time_key", to_date(lit("2024-12-01"), "yyyy-MM-dd"))

  val januaryData = smsReName
    .join(pushReName, Seq("ctn"), "left")
    .join(emailReName, Seq("ctn"), "left")
    .join(outboundReName, Seq("ctn"), "left")
    .join(robotReName, Seq("ctn"), "left")
    .withColumn("time_key", to_date(lit("2025-01-01"), "yyyy-MM-dd"))



  val finalDf = decemberData
    .unionByName(januaryData)
    .unionByName(tableAanaaMvp);

  val superFinalDf = finalDf
    .withColumn("relevant_segm_upsell_outbound", lit(null).cast("string"))
    .unionByName(tableAnaMvp)
    .select(
      col("ctn"),
      col("relevant_segm_tve_sms"),
      col("relevant_segm_umcs_fun_sms"),
      col("relevant_segm_umcs_transfabroad_sms"),
      col("relevant_segm_umcs_appstore_sms"),
      col("relevant_segm_umcs_appgallery_sms"),
      col("relevant_segm_umcs_parking_sms"),
      col("relevant_segm_autopay_mp_sms"),
      col("relevant_segm_secretary_sms"),
      col("relevant_segm_books_sms"),
      col("relevant_segm_cloud_sms"),
      col("relevant_segm_privetplus_sms"),
      col("relevant_segm_common_balance_sms"),
      col("relevant_segm_gamification_beeline_games_sms"),
      col("relevant_segm_gamification_games_external_sms"),
      col("relevant_segm_gamification_retention_sms"),
      col("relevant_segm_umcs_fun_push"),
      col("relevant_segm_umcs_transport_push"),
      col("relevant_segm_umcs_appgallery_push"),
      col("relevant_segm_secretary_push"),
      col("relevant_segm_books_push"),
      col("relevant_segm_cloud_push"),
      col("relevant_segm_common_balance_push"),
      col("relevant_segm_gamification_beeline_games_push"),
      col("relevant_segm_gamification_games_external_push"),
      col("relevant_segm_gamification_quizzes_external_push"),
      col("relevant_segm_gamification_retention_push"),
      col("relevant_segm_books_email"),
      col("relevant_segm_cloud_email"),
      col("relevant_segm_common_balance_email"),
      col("relevant_segm_gamification_beeline_games_email"),
      col("relevant_segm_fmc_outbound"),
      col("relevant_segm_common_balance_outbound"),
      col("relevant_segm_upsell_outbound"),
      col("relevant_segm_fmc_robot"),
      col("time_key"));



}
