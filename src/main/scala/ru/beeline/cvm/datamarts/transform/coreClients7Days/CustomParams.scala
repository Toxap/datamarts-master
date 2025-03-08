package ru.beeline.cvm.datamarts.transform.coreClients7Days

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit{

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  val LOAD_DATE_YYYY_MM_DD = tomorrowDs
  private val LOAD_DATE = LocalDate.parse(LOAD_DATE_YYYY_MM_DD, inputFormat)

  val LOAD_DATE_YYYY_MM_DD_14: String = inputFormat.format(LOAD_DATE.minusDays(14))
  val LOAD_DATE_YYYY_MM_DD_8: String = inputFormat.format(LOAD_DATE.minusDays(8))
  val LOAD_DATE_YYYY_MM_DD_plus1: String = inputFormat.format(LOAD_DATE.plusDays(1))

  val tableFctSubsInflowDailyPub = getAppConf(jobConfigKey, "tableFctSubsInflowDailyPub")
  val tableDimAccountType = getAppConf(jobConfigKey, "tableDimAccountType")
  val tableMvChannelHistPub = getAppConf(jobConfigKey, "tableMvChannelHistPub")
  val tableDmDicMapElemVerHistVPub = getAppConf(jobConfigKey, "tableDmDicMapElemVerHistVPub")
  val tableDimHyMgkpiDimPub = getAppConf(jobConfigKey, "tableDimHyMgkpiDimPub")
  val tableCustomersPub = getAppConf(jobConfigKey, "tableCustomersPub")
  val tableAggCtnSmsCatPubD = getAppConf(jobConfigKey, "tableAggCtnSmsCatPubD")
  val tableCommArpuAllPub = getAppConf(jobConfigKey, "tableCommArpuAllPub")
  val tableDataAllPub = getAppConf(jobConfigKey, "tableDataAllPub")
  val tableAggTimeSpendPositionCodesPubD = getAppConf(jobConfigKey, "tableAggTimeSpendPositionCodesPubD")
  val tableLastPricePlanPub = getAppConf(jobConfigKey, "tableLastPricePlanPub")
  val tableRechargesPub = getAppConf(jobConfigKey, "tableRechargesPub")
  val tableVoiceAllPub = getAppConf(jobConfigKey, "tableVoiceAllPub")
  val tableAggCtnBalancePubD = getAppConf(jobConfigKey, "tableAggCtnBalancePubD")
  val tableCoreClientsBase = getAppConf(jobConfigKey, "tableCoreClientsBase")


}
