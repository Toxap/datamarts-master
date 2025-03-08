package ru.beeline.cvm.datamarts.transform.modelForecastResponsed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class Load extends CustomParams {

  val selectedColumns = Seq(
    "ctn",
    "ban_key",
    "gr_id",
    "product_id",
    "use_case_impact_id",
    "pred_sale_outbound",
    "pred_sale_1c",
    "pred_sale_email",
    "pred_sale_sms",
    "pred_sale_ussd",
    "pred_sale_robot",
    "pred_sale_lk",
    "pred_sale_dok",
    "pred_sale_mp",
    "pred_sale_stories",
    "pred_sale_push",
    "pred_sale_calibrated_outbound",
    "pred_sale_calibrated_1c",
    "pred_sale_calibrated_email",
    "pred_sale_calibrated_sms",
    "pred_sale_calibrated_ussd",
    "pred_sale_calibrated_robot",
    "pred_sale_calibrated_lk",
    "pred_sale_calibrated_dok",
    "pred_sale_calibrated_mp",
    "pred_sale_calibrated_stories",
    "pred_sale_calibrated_push"
  ).map(col)

  val pred_sale_product_1: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct1).select(selectedColumns: _*)
  val pred_sale_product_3: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct3).select(selectedColumns: _*)
  val pred_sale_product_4: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct4).select(selectedColumns: _*)
  val pred_sale_product_6: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct6).select(selectedColumns: _*)
  val pred_sale_product_9: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct9).select(selectedColumns: _*)
  val pred_sale_product_11: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct11).select(selectedColumns: _*)
  val pred_sale_product_12: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct12).select(selectedColumns: _*)
  val pred_sale_product_13: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct13).select(selectedColumns: _*)
  val pred_sale_product_14: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct14).select(selectedColumns: _*)
  val pred_sale_product_15: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct15).select(selectedColumns: _*)
  val pred_sale_product_16: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct16).select(selectedColumns: _*)
  val pred_sale_product_18: DataFrame = getDataFrameWithMaxTimeKey(TablePredSaleProduct18).select(selectedColumns: _*)


}
