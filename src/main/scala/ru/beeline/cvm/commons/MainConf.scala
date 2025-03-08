package ru.beeline.cvm.commons

import org.rogach.scallop.{ScallopConf, ScallopOption}

class MainConf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val packageName: ScallopOption[String] = opt[String](required = false, name = "packageName")
  val appName: ScallopOption[String] = opt[String](required = false, name = "appName")
  val FamilyTarget: ScallopOption[String] = opt[String](required = false, name = "FamilyTarget")
  verify()

}
