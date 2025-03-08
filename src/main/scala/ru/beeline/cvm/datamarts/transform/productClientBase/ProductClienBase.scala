package ru.beeline.cvm.datamarts.transform.productClientBase
import org.apache.spark.sql.functions._
class ProductClienBase extends Load {

  val aggSubsProfilePubD = aggSubsProfile

  val resultDf = aggSubsProfilePubD
    .withColumnRenamed("time_key", "report_dt")
    .filter(
      col("report_dt") === FIRST_DAY_OF_MONTH_YYYY_MM_01 &&
        col("curr_dw_status").isin("A", "S") &&
        !col("dw_segment").isin("99", "TST") &&
        col("account_type") === 13)
    .withColumn("first_ctn_digit", col("ctn").substr(0, 1))
    .filter(col("first_ctn_digit") =!= "0")
    .withColumn("report_dt", to_date(concat(date_format(col("report_dt"), "yyyy-MM"), lit("-20")), "yyyy-MM-dd"))
    .select("ctn", "ban", "report_dt")
}
