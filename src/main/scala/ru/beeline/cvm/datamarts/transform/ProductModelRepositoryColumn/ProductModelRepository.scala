package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}

class ProductModelRepository extends Load {

  val productChannelCostDf = getproductChannelCost(TableProductChannelCost)
  val productAllDf: DataFrame = getProductAll(TablePredSaleProductAll)
  val productModelEffect: DataFrame = getSource(TableProductModelEffect)
  val productTreshholdDf: DataFrame = getProductTreshhold(TableProductTreshhold)

  val outbound = productChannelCostDf.select(col("cost_outbound")).collect()(0)(0)
  val cost1c = productChannelCostDf.select(col("cost_1c")).collect()(0)(0)
  val email = productChannelCostDf.select(col("cost_email")).collect()(0)(0)
  val sms = productChannelCostDf.select(col("cost_sms")).collect()(0)(0)
  val ussd = productChannelCostDf.select(col("cost_ussd")).collect()(0)(0)
  val robot = productChannelCostDf.select(col("cost_robot")).collect()(0)(0)
  val lk = productChannelCostDf.select(col("cost_lk")).collect()(0)(0)
  val dok = productChannelCostDf.select(col("cost_dok")).collect()(0)(0)
  val mp = productChannelCostDf.select(col("cost_mp")).collect()(0)(0)
  val stories = productChannelCostDf.select(col("cost_stories")).collect()(0)(0)
  val push = productChannelCostDf.select(col("cost_push")).collect()(0)(0)

  val productModelRepositoryColumn: DataFrame = productAllDf
    .join(productModelEffect, Seq("product_id"))
    .join(productTreshholdDf, Seq("product_id"))
    .withColumn("pred_effect_outbound", col("pred_effect"))
    .withColumn("pred_effect_1c", col("pred_effect"))
    .withColumn("pred_effect_email", col("pred_effect"))
    .withColumn("pred_effect_sms", col("pred_effect"))
    .withColumn("pred_effect_ussd", col("pred_effect"))
    .withColumn("pred_effect_robot", col("pred_effect"))
    .withColumn("pred_effect_lk", col("pred_effect"))
    .withColumn("pred_effect_dok", col("pred_effect"))
    .withColumn("pred_effect_mp", col("pred_effect"))
    .withColumn("pred_effect_stories", col("pred_effect"))
    .withColumn("pred_effect_push", col("pred_effect"))
    .withColumn("pred_effect_product", col("pred_effect"))
    .withColumn("pred_score_outbound", col("pred_sale_calibrated_outbound") * col("pred_effect_outbound") - outbound)
    .withColumn("pred_score_1c", col("pred_sale_calibrated_1c") * col("pred_effect_1c") - cost1c)
    .withColumn("pred_score_email", col("pred_sale_calibrated_email") * col("pred_effect_email") - email)
    .withColumn("pred_score_sms", col("pred_sale_calibrated_sms") * col("pred_effect_sms") - sms)
    .withColumn("pred_score_ussd", col("pred_sale_calibrated_ussd") * col("pred_effect_ussd") - ussd)
    .withColumn("pred_score_robot", col("pred_sale_calibrated_robot") * col("pred_effect_robot") - robot)
    .withColumn("pred_score_lk", col("pred_sale_calibrated_lk") * col("pred_effect_lk") - lk)
    .withColumn("pred_score_dok", col("pred_sale_calibrated_dok") * col("pred_effect_dok") - dok)
    .withColumn("pred_score_mp", col("pred_sale_calibrated_mp") * col("pred_effect_mp") - mp)
    .withColumn("pred_score_stories", col("pred_sale_calibrated_stories") * col("pred_effect_stories") - stories)
    .withColumn("pred_score_push", col("pred_sale_calibrated_push") * col("pred_effect_push") - push)
    .withColumn("block_reason_outbound",
      when(col("threshold_pred_sale_calibrated_outbound") > col("pred_sale_calibrated_outbound"), 1)
        .when(col("pred_score_outbound") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_1c
    .withColumn("block_reason_1c",
      when(col("threshold_pred_sale_calibrated_1c") > col("pred_sale_calibrated_1c"), 1)
        .when(col("pred_score_1c") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_email
    .withColumn("block_reason_email",
      when(col("threshold_pred_sale_calibrated_email") > col("pred_sale_calibrated_email"), 1)
        .when(col("pred_score_email") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_sms
    .withColumn("block_reason_sms",
      when(col("threshold_pred_sale_calibrated_sms") > col("pred_sale_calibrated_sms"), 1)
        .when(col("pred_score_sms") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_ussd
    .withColumn("block_reason_ussd",
      when(col("threshold_pred_sale_calibrated_ussd") > col("pred_sale_calibrated_ussd"), 1)
        .when(col("pred_score_ussd") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_robot
    .withColumn("block_reason_robot",
      when(col("threshold_pred_sale_calibrated_robot") > col("pred_sale_calibrated_robot"), 1)
        .when(col("pred_score_robot") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_lk
    .withColumn("block_reason_lk",
      when(col("threshold_pred_sale_calibrated_lk") > col("pred_sale_calibrated_lk"), 1)
        .when(col("pred_score_lk") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_dok
    .withColumn("block_reason_dok",
      when(col("threshold_pred_sale_calibrated_dok") > col("pred_sale_calibrated_dok"), 1)
        .when(col("pred_score_dok") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_mp
    .withColumn("block_reason_mp",
      when(col("threshold_pred_sale_calibrated_mp") > col("pred_sale_calibrated_mp"), 1)
        .when(col("pred_score_mp") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_stories
    .withColumn("block_reason_stories",
      when(col("threshold_pred_sale_calibrated_stories") > col("pred_sale_calibrated_stories"), 1)
        .when(col("pred_score_stories") < 0, 2)
        .otherwise(0)
    )
    // Логика для block_reason_push
    .withColumn("block_reason_push",
      when(col("threshold_pred_sale_calibrated_push") > col("pred_sale_calibrated_push"), 1)
        .when(col("pred_score_push") < 0, 2)
        .otherwise(0)
    )
    .select(
      col("ctn"),
      col("gr_id"),
      productAllDf("use_case_impact_id"),
      col("pred_sale_calibrated_outbound").cast("float"),
      col("pred_sale_calibrated_1c").cast("float"),
      col("pred_sale_calibrated_email").cast("float"),
      col("pred_sale_calibrated_sms").cast("float"),
      col("pred_sale_calibrated_ussd").cast("float"),
      col("pred_sale_calibrated_robot").cast("float"),
      col("pred_sale_calibrated_lk").cast("float"),
      col("pred_sale_calibrated_dok").cast("float"),
      col("pred_sale_calibrated_mp").cast("float"),
      col("pred_sale_calibrated_stories").cast("float"),
      col("pred_sale_calibrated_push").cast("float"),
      col("pred_effect_outbound").cast("float"),
      col("pred_effect_1c").cast("float"),
      col("pred_effect_email").cast("float"),
      col("pred_effect_sms").cast("float"),
      col("pred_effect_ussd").cast("float"),
      col("pred_effect_robot").cast("float"),
      col("pred_effect_lk").cast("float"),
      col("pred_effect_dok").cast("float"),
      col("pred_effect_mp").cast("float"),
      col("pred_effect_stories").cast("float"),
      col("pred_effect_push").cast("float"),
      col("pred_score_outbound").cast("float"),
      col("pred_score_1c").cast("float"),
      col("pred_score_email").cast("float"),
      col("pred_score_sms").cast("float"),
      col("pred_score_ussd").cast("float"),
      col("pred_score_robot").cast("float"),
      col("pred_score_lk").cast("float"),
      col("pred_score_dok").cast("float"),
      col("pred_score_mp").cast("float"),
      col("pred_score_stories").cast("float"),
      col("pred_score_push").cast("float"),
      col("block_reason_outbound").cast("int"),
      col("block_reason_1c").cast("int"),
      col("block_reason_email").cast("int"),
      col("block_reason_sms").cast("int"),
      col("block_reason_ussd").cast("int"),
      col("block_reason_robot").cast("int"),
      col("block_reason_lk").cast("int"),
      col("block_reason_dok").cast("int"),
      col("block_reason_mp").cast("int"),
      col("block_reason_stories").cast("int"),
      col("block_reason_push").cast("int"),
      col("product_id")
    )
}
