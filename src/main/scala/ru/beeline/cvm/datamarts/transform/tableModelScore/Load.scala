package ru.beeline.cvm.datamarts.transform.tableModelScore

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Load extends CustomParams {

  def product1: DataFrame = {
    spark.table(TablePredSaleProduct1)
      .withColumn("product", lit("Билайн ТВ"));
  }

  def product2: DataFrame = {
    spark.table(TablePredSaleProduct2)
      .withColumn("product", lit("FMC"))
      .select(
        col("ctn"),
        col("ban_key"),
        col("gr_id"),
        lit(2).as("product_id"),
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
        col("pred_sale_calibrated_push"),
        col("report_dt"),
        col("time_key"),
        col("product"))
  }
  def product3: DataFrame = {
    spark.table(TablePredSaleProduct3)
      .withColumn("product", lit("Развлечения"));
  }
  def product4: DataFrame = {
    spark.table(TablePredSaleProduct4)
      .withColumn("product", lit("Транспорт"));
  }

  def product6: DataFrame = {
    spark.table(TablePredSaleProduct6)
      .withColumn("product", lit("Переводы за рубеж"));
  }

  def product8: DataFrame = {
    spark.table(TablePredSaleProduct8)
      .withColumn("product", lit("App Store"));
  }

  def product9: DataFrame = {
    spark.table(TablePredSaleProduct9)
      .withColumn("product", lit("AppGallery"));
  }

  def product10: DataFrame = {
    spark.table(TablePredSaleProduct10)
      .withColumn("product", lit("Соц сети"));
  }

  def product11: DataFrame = {
    spark.table(TablePredSaleProduct11)
      .withColumn("product", lit("Парковки"));
  }

  def product12: DataFrame = {
    spark.table(TablePredSaleProduct12)
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
        col("pred_sale_calibrated_push"),
        col("report_dt"),
        col("time_key"))
      .withColumn("product", lit("Автоплатеж билайн"));
  }

  def product13: DataFrame = {
    spark.table(TablePredSaleProduct13)
      .withColumn("product", lit("Виртуальный Помощник: платная версия"));
  }

  def product14: DataFrame = {
    spark.table(TablePredSaleProduct14)
      .withColumn("product", lit("Билайн Книги"));
  }

  def product15: DataFrame = {
    spark.table(TablePredSaleProduct15)
      .withColumn("product", lit("Билайн Облако"));
  }

  def product16: DataFrame = {
    spark.table(TablePredSaleProduct16)
      .withColumn("product", lit("Привет"));
  }

  def product18: DataFrame = {
    spark.table(TablePredSaleProduct18)
      .withColumn("product", lit("Общий Баланс"));
  }

  def product23: DataFrame = {
    spark.table(TablePredSaleProduct23)
      .withColumn("product", lit("Геймфикация"));
  }








}
