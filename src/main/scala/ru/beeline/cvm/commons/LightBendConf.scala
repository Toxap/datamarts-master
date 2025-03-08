package ru.beeline.cvm.commons

import com.typesafe.config.{Config, ConfigFactory}

trait LightBendConf {

  def appProps(appName: String): Config = {
    val lConf: Config = ConfigFactory.load()
    lConf.checkValid(ConfigFactory.defaultReference())

    val appConfig: Config = {
      try {
        lConf.getConfig(appName)
      } catch {
        case e: com.typesafe.config.ConfigException => ConfigFactory.empty()
      }
    }
    appConfig
  }
}
