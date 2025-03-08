package ru.beeline.cvm.datamarts.transform.ProductTreshholdModel

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class ProductTreshholdModel extends Load {

      private val ProductTreshholdModelSource: DataFrame = getProductTreshholdModelSource(TableProductTreshholdModelSource)

      val res: DataFrame = ProductTreshholdModelSource
        .filter(lit(LOAD_DATE_YYYY_MM_DD).between(col("effective_from"), col("effective_to")))
        .select(
              col("threshold_pred_sale_calibrated_outbound"),
              col("threshold_pred_sale_calibrated_1c"),
              col("threshold_pred_sale_calibrated_email"),
              col("threshold_pred_sale_calibrated_sms"),
              col("threshold_pred_sale_calibrated_ussd"),
              col("threshold_pred_sale_calibrated_robot"),
              col("threshold_pred_sale_calibrated_lk"),
              col("threshold_pred_sale_calibrated_dok"),
              col("threshold_pred_sale_calibrated_mp"),
              col("threshold_pred_sale_calibrated_stories"),
              col("threshold_pred_sale_calibrated_push"),
                col("product_id"),
                col("use_case_impact_id"),
        )

}
