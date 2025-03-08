package ru.beeline.cvm.datamarts.transform.adsDataset2_0

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableAdsDataset20: String = getAppConf(jobConfigKey, "tableAdsDataset20")
  val TableAggCtnPricePlansHistPubD: String = getAppConf(jobConfigKey, "tableAggCtnPricePlansHistPubD")
  val TableDMMOBILESUBSCRIBERUSAGEDAILY: String = getAppConf(jobConfigKey, "tableDMMOBILESUBSCRIBERUSAGEDAILY")

  val TableAGGCTNBALANCEPERIODPUBM: String = getAppConf(jobConfigKey, "tableAGGCTNBALANCEPERIODPUBM")
}
