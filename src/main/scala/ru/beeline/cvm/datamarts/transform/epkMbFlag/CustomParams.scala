package ru.beeline.cvm.datamarts.transform.epkMbFlag

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import ru.beeline.cvm.commons.CustomParamsInit

class CustomParams extends CustomParamsInit {

    private val inputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    private val outputFormat = DateTimeFormatter.ofPattern("yyyyMM")
    val LOAD_DATE = LocalDate.parse(tomorrowDs, inputFormat)
    val SCORE_DATE = inputFormat.format(LOAD_DATE)
    val SCORE_DATE_4_days = inputFormat.format(LOAD_DATE.minusDays(4))

}
