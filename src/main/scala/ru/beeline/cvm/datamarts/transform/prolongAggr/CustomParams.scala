package ru.beeline.cvm.datamarts.transform.prolongAggr

import ru.beeline.cvm.commons.CustomParamsInit

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class CustomParams extends CustomParamsInit {
    private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
    val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
    val SCORE_DATE = inputFormat.format(LOAD_DATE)
    val SCORE_DATE_1_days = inputFormat.format(LOAD_DATE.minusDays(1))
    val TableProlongCustFeatures: String = getAppConf(jobConfigKey, "tableProlongCustFeatures")
    val TableProlongOutput: String = getAppConf(jobConfigKey, "tableProlongOutput")
    val TableProlongAggr: String = getAppConf(jobConfigKey, "tableProlongAggr")
}
