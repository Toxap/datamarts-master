package ru.beeline.cvm.datamarts.transform.modelForecastResponsed

object Main extends ModelForecastResponsed {

    res
      .coalesce(10)
      .write
      .mode("overwrite")
      .format("orc")
      .insertInto(TablePredSaleProductAll)

}
