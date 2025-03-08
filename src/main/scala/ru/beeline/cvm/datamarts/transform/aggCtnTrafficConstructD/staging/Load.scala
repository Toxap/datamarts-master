package ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD.staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD.CustomParams

class Load extends CustomParams {

  private val listPricePlanKey: List[String] = List("CXK21_1", "EXK21_SAT", "EXK21_1", "VXK21_1", "SXK21_1", "WXK21_1",
    "EXK21RSAT", "EXK21R", "WXK21R", "CXK21R", "SXK21R", "VXK21R",
    "VXK22", "VXK22_2", "VXK22_3", "VXK22_4", "VXK22_5",
    "WXK22", "WXK22_2", "WXK22_3", "WXK22_4", "WXK22_5",
    "SXK22", "SXK22_2", "SXK22_3", "SXK22_4", "SXK22_5",
    "CXK22", "CXK22_2", "CXK22_3", "CXK22_4", "CXK22_5",
    "EXK22", "EXK22_2", "EXK22_3", "EXK22_4", "EXK22_5",
    "EXK22SAT", "EXK22SAT2", "EXK22SAT3", "EXK22SAT4", "EXK22SAT5")
  private val listCurrStatus: List[String] = List("A", "S")
  private val listSegmentKey: List[String] = List(
    "N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08", "N09", "N10", "N11", "N12", "AGN", "EXS"
  )

  def getDimSubscriber: DataFrame = {
    spark.read.table(TableDimSubscriberPub)
      .filter(col("curr_dw_status").isin(listCurrStatus: _*))
      .filter(col("segment_key").isin(listSegmentKey: _*))
      .filter(col("curr_price_plan_key").isin(listPricePlanKey: _*))
      .filter(substring(col("price_plan_change_date"), 1, 10) < LOAD_DATE_YYYY_MM_DD_7)
      .select(
        col("subs_key").as("ctn"),
        col("ban_key").as("ban"),
        col("market_key_src").as("market"),
        substring(col("price_plan_change_date"), 1, 10).as("price_plan_change_date"),
        floor(datediff(lit(LOAD_DATE_YYYY_MM_DD_4),
          substring(col("price_plan_change_date"), 1, 10)) / 7)
          .cast("Int").as("full_week_num")
      )
      .withColumn("end_of_period", expr("date_add(price_plan_change_date,full_week_num*7-1)"))
  }

  def getFctUsagePrepChaN: DataFrame = {
    spark.read.table(TableFctUsagePrepChaNPub)
      .filter(col("call_start_time").between(LOAD_DATE_PYYYYMMDD_29, LOAD_DATE_PYYYYMMDD))
      .filter(col("call_direction_ind") === 2)
      .filter(col("connection_type_key").isin("1", "3", "X"))
      .filter(col("call_type_code") === "V")
      .filter(col("location_type_key").isin("1", "2", "X"))
      .filter(col("roaming_type_key").isin("H", "X"))
      .filter(col("actual_call_duration_sec") > 0)
      .select(
        col("subs_key").as("ctn"),
        col("ban_key").as("ban"),
        substring(col("call_start_time_src"), 1, 10).as("call_start_time_src"),
        col("actual_call_duration_sec"),
        col("rounded_data_volume")
      )
  }

  def getFctUsagePrepOgprsN: DataFrame = {
    spark.read.table(TableFctUsagePrepOgprsNPub)
      .filter(col("call_start_time").between(LOAD_DATE_PYYYYMMDD_29, LOAD_DATE_PYYYYMMDD))
      .filter(col("counted_cdr_ind") === "1")
      .filter(!col("business_service_key").isin(62110, 62107, 62122, 62113, 62119, 62116))
      .filter(col("call_type_code") === "G")
      .filter(col("actual_call_duration_sec") > 0)
      .select(
        col("subs_key").as("ctn"),
        col("ban_key").as("ban"),
        substring(col("call_start_time_src"), 1, 10).as("call_start_time_src"),
        col("actual_call_duration_sec"),
        col("rounded_data_volume")
      )
  }
}
