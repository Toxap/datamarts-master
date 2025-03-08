package ru.beeline.cvm.datamarts.transform.aggCtnTrafficConstructD

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val LOAD_DATE_YYYY_MM_DD_4: String = inputFormat.format(LOAD_DATE.minusDays(4))
  val LOAD_DATE_YYYY_MM_DD_7: String = inputFormat.format(LOAD_DATE.minusDays(7))
  val LOAD_DATE_YYYY_MM_DD_29: String = inputFormat.format(LOAD_DATE.minusDays(29))
  val LOAD_DATE_PYYYYMMDD = "P" + outputFormat.format(LOAD_DATE)
  val LOAD_DATE_PYYYYMMDD_29 = "P" + outputFormat.format(LOAD_DATE.minusDays(29))

  val TableDimSubscriberPub: String = getAppConf(jobConfigKey, "tableDimSubscriberPub")
  val TableFctUsagePrepChaNPub: String = getAppConf(jobConfigKey, "tableFctUsagePrepChaNPub")
  val TableFctUsagePrepOgprsNPub: String = getAppConf(jobConfigKey, "tableFctUsagePrepOgprsNPub")

  val TableAggCtnTrafficConstructD: String = getAppConf(jobConfigKey, "tableAggCtnTrafficConstructD")
}
