package ru.beeline.cvm.datamarts.transform.upsellTarget

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Load extends CustomParams {

    def getCampLibrary: DataFrame = spark.read.table(TableCampLibrary)
      .select(col("camp_id"), col("wave_id"), col("camp_use_case_id"), col("info_channel_id"))

    def getDimDic: DataFrame = spark.read.table(TableDimDic)
      .select(col("isn"), col("name_eng"), col("name_rus"))

    def getWave: DataFrame = spark.read.table(TableWave)
      .filter(col("wave_month") === LOAD_DATE_YYYY_MM_01)
      .select(col("wave_id"))

    def getCmsMembers: DataFrame = spark.read.table(TableNbaMemberSample)
      .filter(col("time_key") === LOAD_DATE_PYYYYMM)
      .filter(col("tg_new") === 1)
      .select(col("last_subs_key"), col("last_ban_key"), col("camp_id"), col("sale_new"))
}
