package ru.beeline.cvm.datamarts.transform.ProductModelRepositoryColumnNonBlock

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, array_contains, col, when}

class ProductModelRepositoryNonBlock extends Load {

  val productModelRepositoryColumn: DataFrame = getProductModelRepository(TableProductModelRepositoryColumn)

  val productModelRepositoryNonBlock = productModelRepositoryColumn
    .withColumn("block_reasons_array",
      array(
        col("block_reason_outbound"),
        col("block_reason_1c"),
        col("block_reason_email"),
        col("block_reason_sms"),
        col("block_reason_ussd"),
        col("block_reason_robot"),
        col("block_reason_lk"),
        col("block_reason_dok"),
        col("block_reason_mp"),
        col("block_reason_stories"),
        col("block_reason_push")
      )
    )
    .withColumn("has_no_block", array_contains(col("block_reasons_array"), 0))

  val updatedProductModelRepository = productModelRepositoryNonBlock
    .withColumn("pred_score_outbound", when(col("has_no_block") && col("block_reason_outbound") =!= 0, -99999).otherwise(col("pred_score_outbound")))
    .withColumn("pred_score_1c", when(col("has_no_block") && col("block_reason_1c") =!= 0, -99999).otherwise(col("pred_score_1c")))
    .withColumn("pred_score_email", when(col("has_no_block") && col("block_reason_email") =!= 0, -99999).otherwise(col("pred_score_email")))
    .withColumn("pred_score_sms", when(col("has_no_block") && col("block_reason_sms") =!= 0, -99999).otherwise(col("pred_score_sms")))
    .withColumn("pred_score_ussd", when(col("has_no_block") && col("block_reason_ussd") =!= 0, -99999).otherwise(col("pred_score_ussd")))
    .withColumn("pred_score_robot", when(col("has_no_block") && col("block_reason_robot") =!= 0, -99999).otherwise(col("pred_score_robot")))
    .withColumn("pred_score_lk", when(col("has_no_block") && col("block_reason_lk") =!= 0, -99999).otherwise(col("pred_score_lk")))
    .withColumn("pred_score_dok", when(col("has_no_block") && col("block_reason_dok") =!= 0, -99999).otherwise(col("pred_score_dok")))
    .withColumn("pred_score_mp", when(col("has_no_block") && col("block_reason_mp") =!= 0, -99999).otherwise(col("pred_score_mp")))
    .withColumn("pred_score_stories", when(col("has_no_block") && col("block_reason_stories") =!= 0, -99999).otherwise(col("pred_score_stories")))
    .withColumn("pred_score_push", when(col("has_no_block") && col("block_reason_push") =!= 0, -99999).otherwise(col("pred_score_push")))
    .select(
      col("ctn"),
      col("gr_id"),
      col("use_case_impact_id"),
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
