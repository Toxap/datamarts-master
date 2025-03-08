package ru.beeline.cvm.datamarts.transform.bmaAatDbLteChangeSim
import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableTCamps: String = getAppConf(jobConfigKey,"tableTCamps")
  val TableCmsMembers: String = getAppConf(jobConfigKey, "tableCmsMembers")
  val TableAggTimeSpendPositionCodesPubD: String = getAppConf(jobConfigKey, "tableAggTimeSpendPositionCodesPubD")
  val TableBmaAatLoginRefarm: String = getAppConf(jobConfigKey, "tableBmaAatLoginRefarm")
  val TableStgSimLteDaily: String = getAppConf(jobConfigKey, "tableStgSimLteDaily")
  val TableStgDeviceChanged: String = getAppConf(jobConfigKey, "tableStgDeviceChanged")
  val TableUsage: String = getAppConf(jobConfigKey, "tableUsage")
  val TableBmaAatDbLteChangeSim: String = getAppConf(jobConfigKey, "tableBmaAatDbLteChangeSim")




}