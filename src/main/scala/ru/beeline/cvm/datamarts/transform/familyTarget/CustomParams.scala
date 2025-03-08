package ru.beeline.cvm.datamarts.transform.familyTarget

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters.{firstDayOfMonth, lastDayOfMonth}

class CustomParams extends CustomParamsInit {

  private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
  private val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)

  val LOAD_DATE_YYYY_MM_01 = inputFormat.format(LOAD_DATE.withDayOfMonth(1))
  val LOAD_DATE_YYYY_MM_DD_FIRST_DAY = inputFormat.format(LOAD_DATE.minusMonths(1).`with`(firstDayOfMonth()))
  val LOAD_DATE_YYYY_MM_DD_LAST_DAY = inputFormat.format(LOAD_DATE.minusMonths(1).`with`(lastDayOfMonth()))
  val LOAD_DATE_PYYYYMM = "P" + outputFormat.format(LOAD_DATE.minusMonths(1))

  lazy val TableCampLibrary = getAppConf(jobConfigKey, "tableCampLibrary")
  val TableDimDic = getAppConf(jobConfigKey, "tableDimDic")
  val TableWave = getAppConf(jobConfigKey, "tableWave")
  val TableNbaMemberSample = getAppConf(jobConfigKey, "tableNbaMemberSample")
  val TableFamilyService = getAppConf(jobConfigKey, "tableFamilyService")
  val TableFamilyTarget = getAppConf(jobConfigKey, "tableFamilyTarget")

}

