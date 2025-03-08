package ru.beeline.cvm.datamarts.transform.familyTarget

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.time.format.DateTimeFormatter
import ru.beeline.cvm.commons.CustomDatamartsParams

import java.time.temporal.TemporalAdjusters.{firstDayOfMonth, lastDayOfMonth}

class FamilyTarget(implicit spark: SparkSession, params: CustomDatamartsParams) extends App {

  private[this] val load: Load = new Load()
  val LOAD_DATE_YYYY_MM_DD = params.LOAD_DATE_YYYY_MM_DD
  val LOAD_DATE_YYYY_MM_DD_FIRST_DAY = DateTimeFormatter
    .ofPattern("yyyy-MM-dd").format(LOAD_DATE_YYYY_MM_DD.`with`(firstDayOfMonth()))
  val LOAD_DATE_YYYY_MM_DD_LAST_DAY = DateTimeFormatter
    .ofPattern("yyyy-MM-dd").format(LOAD_DATE_YYYY_MM_DD.`with`(lastDayOfMonth()))
  val LOAD_DATE_PYYYYMM = "P" + DateTimeFormatter
    .ofPattern("yyyy-MM").format(params.LOAD_DATE_YYYY_MM_DD.minusMonths(1))

  def resultDF: DataFrame = {

    val campLibrary = load.getCampLibrary(params.getValue("tableCampLibrary"))
    val dimDic = load.getDimDic(params.getValue("tableDimDic"))
    val wave = load.getWave(params.getValue("tableWave"), LOAD_DATE_YYYY_MM_DD_FIRST_DAY, LOAD_DATE_YYYY_MM_DD_LAST_DAY)
    val iviNbaSample = load.getIviNbaSample("tableNbaMemberSample", LOAD_DATE_PYYYYMM)
    val stgFamilyService = load.getFamilyService(
      "tableFamilyService",
      LOAD_DATE_YYYY_MM_DD_FIRST_DAY,
      LOAD_DATE_YYYY_MM_DD_LAST_DAY)


    val camp = campLibrary.as("cl1")
      .join(wave, Seq("wave_id"), "inner")
      .join(dimDic.as("dd1"), col("cl1.camp_use_case_id") === col("dd1.isn"), "left")
      .join(dimDic.as("dd2"), col("cl1.info_channel_id") === col("dd2.isn"), "left")
      .filter(col("dd1.name_eng").isin("family development", "family for free", "family new donor"))
      .filter(col("dd1.name_rus") =!= "Outbound CRM")
      .select(col("cl1.camp_id"));

    val nba = iviNbaSample.join(camp, Seq("camp_id"), "inner")
      .select("last_subs_key", "last_ban_key", "market_key", "time_key", "camp_id");


    val res = nba.join(stgFamilyService.as("fam"), nba("last_subs_key") === stgFamilyService("ctn"), "left")
      .select(
        col("last_subs_key"),
        col("camp_id"),
        col("last_ban_key"),
        col("market_key"),
        coalesce(col("target"),
          lit(0)).as("target"),
        nba("time_key")
      );

    val camps = res
      .filter(col("target") === 1)
      .select(col("camp_id"))
      .distinct().collect().map(_(0));

    val w = Window.partitionBy(res.col("camp_id"));

    res
      .filter(col("camp_id").isin(camps: _*))
      .withColumn("s", sum(col("target")).over(w))
      .withColumn("c", count(col("target")).over(w))
      .withColumn("resp", col("s") / col("c"))
      .filter(col("resp") < 0.2)
      .select(col("last_subs_key"), col("last_ban_key"), col("market_key"), col("target"), col("time_key"));


  }

}
