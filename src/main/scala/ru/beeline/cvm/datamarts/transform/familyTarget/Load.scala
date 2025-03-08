package ru.beeline.cvm.datamarts.transform.familyTarget

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

class Load(implicit spark: SparkSession) {

  def getCampLibrary(TableCampLibrary: String): DataFrame = {
    spark.table(TableCampLibrary)
      .select(
        col("camp_id"),
        col("wave_id"),
        col("camp_use_case_id"),
        col("info_channel_id"))
  }

  def getDimDic(TableDimDic: String): DataFrame = {
    spark.table(TableDimDic)
      .select(
        col("isn"),
        col("name_eng"),
        col("name_rus"))
  }

  def getWave(TableWave: String, LOAD_DATE_YYYY_MM_DD_FIRST_DAY: String, LOAD_DATE_YYYY_MM_DD_LAST_DAY: String): DataFrame = {
    spark.table(TableWave)
      .filter(col("wave_month") >= LOAD_DATE_YYYY_MM_DD_FIRST_DAY)
      .filter(col("wave_month") <= LOAD_DATE_YYYY_MM_DD_LAST_DAY)
      .select(
        col("wave_id"))
  }

  def getIviNbaSample(TableNbaMemberSample: String, LOAD_DATE_PYYYYMM: String): DataFrame = {
    spark.table(TableNbaMemberSample)
      .filter(col("time_key") === LOAD_DATE_PYYYYMM)
      .select(
        col("last_subs_key"),
        col("last_ban_key"),
        col("market_key"),
        col("camp_id"),
        col("time_key"))
  }


  def getFamilyService(TableFamilyService: String,
                       LOAD_DATE_YYYY_MM_DD_FIRST_DAY: String,
                       LOAD_DATE_YYYY_MM_DD_LAST_DAY: String): DataFrame = {

    val windowSpec = Window.partitionBy(col("ctn")).orderBy(col("time_key"))

    spark.table(TableFamilyService)
      .filter(col("time_key") >= LOAD_DATE_YYYY_MM_DD_FIRST_DAY)
      .filter(col("time_key") <= LOAD_DATE_YYYY_MM_DD_LAST_DAY)
      .withColumn("prev_cnt_recipient", lag(col("cnt_recipients"), 1).over(windowSpec))
      .na.fill(0)
      .withColumn("dif_rec", col("prev_cnt_recipient") - col("cnt_recipients"))
      .withColumn("dif_rec", when(col("dif_rec") >= 1, 1).otherwise(0))
      .withColumn("old_eq_zero", when(col("cnt_recipients") === 0, 1).otherwise(0))
      .withColumn("target", col("dif_rec") * col("old_eq_zero"))
      .filter(col("target") === 1)
      .withColumn("cnt", count("*").over(Window.partitionBy(col("ctn"), date_format(col("time_key"), "yyyy-MM"))))
      .filter(col("cnt") <= 2)
      .select("ctn", "time_key", "target");
  }
}
