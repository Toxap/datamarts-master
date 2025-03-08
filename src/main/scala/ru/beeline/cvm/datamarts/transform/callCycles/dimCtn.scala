package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object dimCtn extends Load {

  def getFirstCtnFirstBanDF = {
    tableDmMobileAttr
      .select(
        col("first_subscriber").as("first_ctn"),
        col("first_ban"),
        col("subscriber_sk")
      )
  }

  baseB2c22Table
    .filter(col("calendar_dt") >= LOAD_DATE_MINUS_3_MONTHS)
    .filter(col("test_price_plan_flg") === 0)
    .filter(col("test_ban_flg") === 0)
    .filter(col("test_account_flg") === 0)
    .filter(!col("segment_cd").isin("-99", "TST"))
    .filter(col("business_type_cd") === "B2C")
    .select(
      col("subscriber_num").as("subs_key"),
      col("ban_num").as("ban_key"),
      col("subscriber_sk"),
      concat(col("calendar_dt").substr(0, 8), lit("01")).as("time_key_src"),
      col("calendar_dt")
    )
    .withColumn("rn", row_number().over(Window.partitionBy(col("subscriber_sk"), col("time_key_src")).orderBy(col("calendar_dt").desc)))
    .filter(col("rn") === 1)
    .join(getFirstCtnFirstBanDF, Seq("subscriber_sk"))
    .select(
      col("subscriber_sk"),
      col("subs_key"),
      col("ban_key"),
      col("first_ctn"),
      col("first_ban"),
      col("time_key_src")
    )
    .repartition(10)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableDimCtnBan);

}
