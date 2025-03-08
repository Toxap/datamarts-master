package ru.beeline.cvm.datamarts.transform.ProductNonmodelRepositoryColumn

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class ProductNonmodelRepository extends Load {

  val productChannelCostDf = getproductChannelCost(TableProductChannelCost)
  val productAnalytPredSale: DataFrame = getSource(TableProductAnalytPredSaleSource)
  val productModelEffect: DataFrame = getproductModelEffect(TableProductModelEffectSource)
    .drop("use_case_impact_id")

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

  val productNonmodelRepository: DataFrame = productAnalytPredSale
    .join(productModelEffect, Seq("product_id"))
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
    .withColumn("pred_score_outbound", col("pred_sale_analyt_outbound") * col("pred_effect_outbound") - outbound)
    .withColumn("pred_score_1c", col("pred_sale_analyt_1c") * col("pred_effect_1c") - cost1c)
    .withColumn("pred_score_email", col("pred_sale_analyt_email") * col("pred_effect_email") - email)
    .withColumn("pred_score_sms", col("pred_sale_analyt_sms") * col("pred_effect_sms") - sms)
    .withColumn("pred_score_ussd", col("pred_sale_analyt_ussd") * col("pred_effect_ussd") - ussd)
    .withColumn("pred_score_robot", col("pred_sale_analyt_robot") * col("pred_effect_robot") - robot)
    .withColumn("pred_score_lk", col("pred_sale_analyt_lk") * col("pred_effect_lk") - lk)
    .withColumn("pred_score_dok", col("pred_sale_analyt_dok") * col("pred_effect_dok") - dok)
    .withColumn("pred_score_mp", col("pred_sale_analyt_mp") * col("pred_effect_mp") - mp)
    .withColumn("pred_score_stories", col("pred_sale_analyt_stories") * col("pred_effect_stories") - stories)
    .withColumn("pred_score_push", col("pred_sale_analyt_push") * col("pred_effect_push") - push)
    .drop("effective_to", "effective_from")
}
