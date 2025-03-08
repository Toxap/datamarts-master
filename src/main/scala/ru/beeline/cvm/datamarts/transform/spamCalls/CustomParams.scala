package ru.beeline.cvm.datamarts.transform.spamCalls

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMMdd")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_DD: String = inputFormat.format(LOAD_DATE)

  val NUM_PARTITIONS = getAppConf(jobConfigKey, "coalesce").toInt

  val TableDmMobileSubscriberAttr: String = getAppConf(jobConfigKey, "tableDmMobileSubscriberAttr")
  val TableDmMobileSubscriber: String = getAppConf(jobConfigKey, "tableDmMobileSubscriber")
  val TableDmMobileSubscriberMonthly: String = getAppConf(jobConfigKey, "tableDmMobileSubscriberMonthly")
  val TableBiisFixTrafUnion: String = getAppConf(jobConfigKey, "tableBiisFixTrafUnion")
  val TableAntispamBinaryBaseScoring: String = getAppConf(jobConfigKey, "tableAntispamBinaryBaseScoring")
  val TableSpamCalls: String = getAppConf(jobConfigKey, "tableSpamCalls")

  val SCORE_DATE_MINUS_ONE_YEAR = inputFormat.format(LOAD_DATE.minusYears(1))
  val MIN_SCORE_DATE_MINUS_TWO_YEAR  = inputFormat.format(LOAD_DATE.minusYears(2))
  val MAX_SCORE_DATE_MINUS_TWO_YEAR  = inputFormat.format(LOAD_DATE.minusYears(2))
}
