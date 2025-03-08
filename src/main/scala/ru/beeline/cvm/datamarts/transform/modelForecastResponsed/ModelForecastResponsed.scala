package ru.beeline.cvm.datamarts.transform.modelForecastResponsed

import org.apache.spark.sql.functions.col

class ModelForecastResponsed extends Load {

  val res = pred_sale_product_1
    .unionAll(pred_sale_product_3)
    .unionAll(pred_sale_product_4)
    .unionAll(pred_sale_product_6)
    .unionAll(pred_sale_product_9)
    .unionAll(pred_sale_product_11)
    .unionAll(pred_sale_product_12)
    .unionAll(pred_sale_product_13)
    .unionAll(pred_sale_product_14)
    .unionAll(pred_sale_product_15)
    .unionAll(pred_sale_product_16)
    .unionAll(pred_sale_product_18)
    .select(
        col("ctn"),
        col("ban_key"),
        col("gr_id"),
        col("product_id"),
        col("use_case_impact_id"),
        col("pred_sale_outbound"),
        col("pred_sale_1c"),
        col("pred_sale_email"),
        col("pred_sale_sms"),
        col("pred_sale_ussd"),
        col("pred_sale_robot"),
        col("pred_sale_lk"),
        col("pred_sale_dok"),
        col("pred_sale_mp"),
        col("pred_sale_stories"),
        col("pred_sale_push"),
        col("pred_sale_calibrated_outbound"),
        col("pred_sale_calibrated_1c"),
        col("pred_sale_calibrated_email"),
        col("pred_sale_calibrated_sms"),
        col("pred_sale_calibrated_ussd"),
        col("pred_sale_calibrated_robot"),
        col("pred_sale_calibrated_lk"),
        col("pred_sale_calibrated_dok"),
        col("pred_sale_calibrated_mp"),
        col("pred_sale_calibrated_stories"),
        col("pred_sale_calibrated_push")
    )

}
