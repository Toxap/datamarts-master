package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.time.LocalDate
import java.time.format.DateTimeFormatter


object DimCtnHist extends Load {
  val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val date_list_2022 = List(
    "2022-01-01",
    "2022-02-01",
    "2022-03-01",
    "2022-04-01",
    "2022-05-01",
    "2022-06-01",
    "2022-07-01",
    "2022-08-01",
    "2022-09-01",
    "2022-10-01",
    "2022-11-01",
    "2022-12-01"
  )

  val date_list_2023_2024 = List(
    "2023-01-01",
    "2023-02-01",
    "2023-03-01",
    "2023-04-01",
    "2023-05-01",
    "2023-06-01",
    "2023-07-01",
    "2023-08-01",
    "2023-09-01",
    "2023-10-01",
    "2023-11-01",
    "2023-12-01",
    "2024-01-01",
    "2024-02-01",
    "2024-03-01",
    "2024-04-01",
    "2024-05-01",
    "2024-06-01",
    "2024-07-01",
    "2024-08-01",
    "2024-09-01",
  )

  def getFirstCtnFirstBanDF = {
    tableDmMobileAttr
      .select(
        col("first_subscriber").as("first_ctn"),
        col("first_ban"),
        col("subscriber_sk")
      )
  }

  def getCtnDimMonth2022(startDate: String) = {
    val START_DATE_LOCAL_DATE = LocalDate.parse(startDate, inputFormat)
    val END_DATE = START_DATE_LOCAL_DATE.plusMonths(1).toString

    val subscriberSkCtnBanDF = baseB2c22Table
      .filter(col("calendar_dt") >= startDate and col("calendar_dt") < END_DATE)
      .filter(col("test_price_plan_flg") === 0)
      .filter(col("test_ban_flg") === 0)
      .filter(col("test_account_flg") === 0)
      .filter(!col("segment_cd").isin("-99", "TST"))
      .filter(col("business_type_cd") === "B2C")
      .select(
        col("subscriber_num").as("subs_key"),
        col("ban_num").as("ban_key"),
        col("subscriber_sk"),
        lit(startDate).as("time_key_src"),
        col("calendar_dt")
      )
      .withColumn("rn", row_number().over(Window.partitionBy(col("subscriber_sk"), col("time_key_src")).orderBy(col("calendar_dt").desc)))
      .filter(col("rn") === 1)
      .select(
        col("subs_key"),
        col("ban_key"),
        col("subscriber_sk"),
        col("time_key_src")
      )

    subscriberSkCtnBanDF
      .join(getFirstCtnFirstBanDF, Seq("subscriber_sk"))
      .select(
        col("subscriber_sk"),
        col("subs_key"),
        col("ban_key"),
        col("first_ctn"),
        col("first_ban"),
        col("time_key_src")
      )
  }

  def getCtnDimMonth20232024(date: String) = {
    baseB2c23Table
      .filter(col("calendar_dt") === date)
      .filter(col("test_price_plan_flg") === 0)
      .filter(col("test_ban_flg") === 0)
      .filter(col("test_account_flg") === 0)
      .filter(!col("segment_cd").isin("-99", "TST"))
      .filter(col("business_type_cd") === "B2C")
      .select(
        col("subscriber_num").as("subs_key"),
        col("ban_num").as("ban_key"),
        col("subscriber_sk"),
        col("calendar_dt").as("time_key_src")
      )
      .join(getFirstCtnFirstBanDF, Seq("subscriber_sk"))
      .select(
        col("subscriber_sk"),
        col("subs_key"),
        col("ban_key"),
        col("first_ctn"),
        col("first_ban"),
        col("time_key_src")
      )
  }

  for (date <- date_list_2022) {
    getCtnDimMonth2022(date)
      .repartition(10)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableDimCtnBan);
  }

  for (date <- date_list_2023_2024) {
    getCtnDimMonth20232024(date)
      .repartition(10)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TableDimCtnBan);
  }

}
