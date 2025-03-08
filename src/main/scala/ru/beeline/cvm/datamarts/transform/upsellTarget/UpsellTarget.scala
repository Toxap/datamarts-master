package ru.beeline.cvm.datamarts.transform.upsellTarget

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UpsellTarget extends Load {

  def transform(campLibrary: DataFrame,
                dimDic: DataFrame,
                wave: DataFrame,
                cmsMembers: DataFrame): DataFrame = {

    val camp: DataFrame = campLibrary.as("cl")
      .join(wave, Seq("wave_id"))
      .join(dimDic.as("dd"), col("cl.camp_use_case_id") === col("dd.isn"), "left")
      .join(dimDic.as("dd1"), col("cl.info_channel_id") === col("dd1.isn"), "left")
      .filter(col("dd1.name_rus") === "Outbound CRM")
      .filter(col("dd.name_eng").isin("flat2bundle", "overspend", "single and lowend", "best tariff offer"))
      .select(col("camp_id"), col("dd.name_eng").as("use_case"))

    cmsMembers
      .join(camp, Seq("camp_id"))
      .select(
        col("last_subs_key"),
        col("last_ban_key"),
        col("use_case"),
        coalesce(col("sale_new"), lit(0)).as("sale_new"),
        lit(LOAD_DATE_PYYYYMM)
      )
  }

  val campLibrary = getCampLibrary
  val dimDic = getDimDic
  val wave = getWave
  val cmsMembers = getCmsMembers

  transform(campLibrary, dimDic, wave, cmsMembers)
    .repartition(1)
    .write
    .format("orc")
    .mode("overwrite")
    .insertInto(TableUpsellTarget)
}
