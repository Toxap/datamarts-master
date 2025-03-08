package ru.beeline.cvm.commons

import org.apache.spark.sql.SparkSession

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomDatamartsParams(implicit spark: SparkSession) extends Params {

  val tomorrowDs: String = getValue("tomorrowDs")
  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  val LOAD_DATE_YYYY_MM_DD: LocalDate = LocalDate.parse(tomorrowDs, inputFormat)

}
