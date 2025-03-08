package ru.beeline.cvm.datamarts.transform.upsell2plusDataset

import org.apache.spark.sql.SparkSession
import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_01 = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val LOAD_DATE_PYYYYMM = "P" + outputFormat.format(LOAD_DATE)
  val LOAD_DATE_PYYYYMM_2M = "P" + outputFormat.format(LOAD_DATE.minusMonths(2))
  val LOAD_DATE_PYYYYMM_4M = "P" + outputFormat.format(LOAD_DATE.minusMonths(4))

  val tableFctActClustSubsM = getAppConf(jobConfigKey, "tableFctActClustSubsM")
  val tableDimBan = getAppConf(jobConfigKey, "tableDimBan")
  val tableAggArpuMou = getAppConf(jobConfigKey, "tableAggArpuMou")
  val tableDimSocParameter = getAppConf(jobConfigKey, "tableDimSocParameter")
  val tableDimSubscriberPub: String = getAppConf(jobConfigKey, "tableDimSubscriberPub")
  val tableUpsellDataset = getAppConf(jobConfigKey, "tableUpsellDataset")
  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  SparkSession.builder()
}
