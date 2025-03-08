package ru.beeline.cvm.datamarts.transform.modelForecastResponsed

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val TablePredSaleProduct1: String = getAppConf(jobConfigKey, "tablePredSaleProduct1")
  val TablePredSaleProduct3: String = getAppConf(jobConfigKey, "tablePredSaleProduct3")
  val TablePredSaleProduct4: String = getAppConf(jobConfigKey, "tablePredSaleProduct4")
  val TablePredSaleProduct6: String = getAppConf(jobConfigKey, "tablePredSaleProduct6")
  val TablePredSaleProduct8: String = getAppConf(jobConfigKey, "tablePredSaleProduct8")
  val TablePredSaleProduct9: String = getAppConf(jobConfigKey, "tablePredSaleProduct9")
  val TablePredSaleProduct10: String = getAppConf(jobConfigKey, "tablePredSaleProduct10")
  val TablePredSaleProduct11: String = getAppConf(jobConfigKey, "tablePredSaleProduct11")
  val TablePredSaleProduct12: String = getAppConf(jobConfigKey, "tablePredSaleProduct12")
  val TablePredSaleProduct13: String = getAppConf(jobConfigKey, "tablePredSaleProduct13")
  val TablePredSaleProduct14: String = getAppConf(jobConfigKey, "tablePredSaleProduct14")
  val TablePredSaleProduct15: String = getAppConf(jobConfigKey, "tablePredSaleProduct15")
  val TablePredSaleProduct16: String = getAppConf(jobConfigKey, "tablePredSaleProduct16")
  val TablePredSaleProduct18: String = getAppConf(jobConfigKey, "tablePredSaleProduct18")
  val TablePredSaleProductAll: String = getAppConf(jobConfigKey, "tablePredSaleProductAll")

  def getDataFrameWithMaxTimeKey(tableName: String): DataFrame = {
    spark.table(tableName)
      .filter(col("time_key") === "2024-12-01")
      .drop("report_dt", "time_key")
  }

}
