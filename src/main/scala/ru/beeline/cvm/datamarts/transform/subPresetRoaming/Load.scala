package ru.beeline.cvm.datamarts.transform.subPresetRoaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, trim}
class Load extends CustomParams {

  def readMobileSubscriberUsageDaily: DataFrame = {
    spark.table(TableDmMobileSubscriberUsageDayly)
  }

  def readFilteredMobileSubscriber: DataFrame = {
    spark.table(TableDmMobileSubscriber)
      .filter(col("calendar_dt") === SCORE_DATE_4)
      .select(
        trim(col("subscriber_num")).as("subscriber_num"),
        trim(col("ban_num")).as("ban_num"))
  }
}
