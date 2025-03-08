package ru.beeline.cvm.datamarts.transform.fctCampEffects

import org.apache.spark.sql.SaveMode

object MainAvg extends FctAvgCampEffect {

  finalDF
    .coalesce(3)
    .write
    .mode(SaveMode.Overwrite)
    .format("orc")
    .insertInto(TableFctAvgCampEffect)

}
