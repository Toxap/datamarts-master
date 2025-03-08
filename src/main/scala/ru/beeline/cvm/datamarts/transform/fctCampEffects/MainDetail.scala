package ru.beeline.cvm.datamarts.transform.fctCampEffects

import org.apache.spark.sql.SaveMode

object MainDetail extends FctDetailCampEffect {

  sdf_detail_new
    .coalesce(3)
    .write
    .mode(SaveMode.Overwrite)
    .format("orc")
    .insertInto(TableFctDetailCampEffect)

}
