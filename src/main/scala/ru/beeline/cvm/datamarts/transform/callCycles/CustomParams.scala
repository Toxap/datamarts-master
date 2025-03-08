package ru.beeline.cvm.datamarts.transform.callCycles

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val startMonthFormat = DateTimeFormatter.ofPattern("yyyy-MM-01")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)
  val LOAD_DATE_MINUS_3_MONTHS: String = LOAD_DATE.minusMonths(3).format(startMonthFormat)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableIntrxnPub: String = getAppConf(jobConfigKey, "tableIntrxnPub")
  val TableFinAccntPub: String = getAppConf(jobConfigKey, "tableFinAccntPub")
  val TableTopicPub: String = getAppConf(jobConfigKey, "tableTopicPub")
  val TableAggCtnChurnPaidPubM: String = getAppConf(jobConfigKey, "tableAggCtnChurnPaidPubM")
  val TableOutbCrmBase202101V2: String = getAppConf(jobConfigKey, "tableOutbCrmBase202101V2")
  val TableSmCsBase2: String = getAppConf(jobConfigKey, "tableSmCsBase2")
  val TableSmCsBase22: String = getAppConf(jobConfigKey, "tableSmCsBase22")
  val TableSmCsCycles2: String = getAppConf(jobConfigKey, "tableSmCsCycles2")
  val TableCallCyclesPrev2: String = getAppConf(jobConfigKey, "tableCallCyclesPrev2")
  val TableSmCsCyclesNext:  String = getAppConf(jobConfigKey, "tableSmCsCyclesNext")
  val TableSmCsCyclesFull:  String = getAppConf(jobConfigKey, "tableSmCsCyclesFull")
  val TableSmCsCyclesDataV2: String = getAppConf(jobConfigKey, "tableSmCsCyclesDataV2")
  val TableSmCsCyclesData2: String = getAppConf(jobConfigKey, "tableSmCsCyclesData2")
  val TableCallCyclesPrevsV2: String = getAppConf(jobConfigKey, "tableCallCyclesPrevsV2")
  val TableCallCyclesData3V2: String = getAppConf(jobConfigKey, "tableCallCyclesData3V2")
  val TableCallCycles: String = getAppConf(jobConfigKey, "tableCallCycles")
  val TableDmMobileSubscriberAttr: String = getAppConf(jobConfigKey, "tableDmMobileSubscriberAttr")
  val TableDmMobileSubscriber: String = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val TableDmMobileSubscriberMonthly: String = getAppConf(jobConfigKey, "tableDmMobileSubscriberMonthly")
  val TableDimCtnBan: String = getAppConf(jobConfigKey, "tableDimCtnBanCallCycle")
}
