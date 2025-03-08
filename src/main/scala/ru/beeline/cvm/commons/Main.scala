package ru.beeline.cvm.commons

import ru.beeline.cvm.datamarts.transform.familyTarget.FamilyTarget

object Main extends RunnableSpark {

  if (args.length < 1) {
    throw new IllegalArgumentException("Class name must be provided as the first argument")
  }

  val params = new CustomDatamartsParams()

  classNameFamilyTarget match {
    case "FamilyTarget" =>
      val familyTarget = new FamilyTarget()(spark, params)
      familyTarget.resultDF
        .repartition(1)
        .write
        .mode("overwrite")
        .format("orc")
        .insertInto("nba_engine.family_target")

    case _ =>
      throw new IllegalArgumentException(s"Unknown class name: $classNameFamilyTarget")
  }

  spark.stop()

}
