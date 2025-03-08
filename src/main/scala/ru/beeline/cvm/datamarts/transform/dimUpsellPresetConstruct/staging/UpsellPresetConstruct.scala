package ru.beeline.cvm.datamarts.transform.dimUpsellPresetConstruct.staging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class UpsellPresetConstruct extends Load {

  def usageWithFamily(usage: DataFrame, recipient: DataFrame): DataFrame = {
    recipient.as("r")
    .join(usage.as("u"), col("r.recipient") === col("u.ctn"), "full")
    .groupBy(coalesce(col("r.ctn"), col("u.ctn")).as("ctn"))
    .agg(
      sum(col("voice_usage")).as("voice_usage"),
      sum(col("data_usage")).as("data_usage"))
  }

  val dmMobileSubscriber = getDmMobileSubscriber
  val dimSocParameter = getDimSocParameter
  val stgFamilyService = getStgFamilyService
  val dmMobileSubscriberUsageDaily = getDmMobileSubscriberUsageDaily
  val aggCtnBasePubD = getAggCtnBasePubD
  val vPpConstructorParam = getVPpConstructorParam
  val serviceAgreementPub = getServiceAgreementPub
  val usage = usageWithFamily(dmMobileSubscriberUsageDaily, stgFamilyService)

  val tmpRes = dmMobileSubscriber.as("subs")
    .join(usage.as("subs_usage"), Seq("ctn"), "left")
    .join(aggCtnBasePubD.as("subs_profile"), Seq("ctn", "ban"), "left")
    .join(dimSocParameter.as("soc_parameter"),
      col("subs.curr_price_plan_key") === col("soc_parameter.soc_code") &&
        col("subs.market") === col("soc_parameter.market_key_dim_soc"),
      "left")
    .join(serviceAgreementPub.as("serv"), Seq("ctn", "ban"), "left")
    .join(vPpConstructorParam.as("const_soc"), col("subs.market") === col("const_soc.market_key"))
    .select(
      col("subs.ctn"),
      col("subs.ban"),
      col("subs.market"),
      col("subs.curr_price_plan_key"),
      when(col("serv.curr_voice_fill").isNull, coalesce(col("soc_parameter.minutes"), lit(0)))
        .otherwise(col("serv.curr_voice_fill")).as("curr_voice_fill"),
      when(col("serv.curr_data_fill").isNull, coalesce(col("soc_parameter.gb"), lit(0)))
        .otherwise(col("serv.curr_data_fill")).as("curr_data_fill"),
      (coalesce(col("subs_usage.voice_usage"), lit(0)) / 84 * 30).as("avg_voice_traffic"),
      (coalesce(col("subs_usage.data_usage"), lit(0)) / 84 * 30).as("avg_data_traffic"),
      coalesce(col("subs_profile.base"), lit(0)).as("arpau"),
      col("const_soc.monthly_fee"),
      col("const_soc.constructor_id"),
      col("const_soc.voice_traffic"),
      col("const_soc.data_traffic"))
    .filter(col("monthly_fee").between(col("arpau") * 1.1, col("arpau") * 2.0))
    .filter(col("curr_voice_fill") < col("voice_traffic"))
    .filter((col("curr_data_fill") =!= 999999 && col("curr_data_fill") < col("data_traffic"))
      || col("curr_data_fill") === 999999)
    .filter(col("avg_data_traffic") < col("data_traffic") * 0.95)
    .filter(col("avg_voice_traffic") < col("voice_traffic") * 0.95)
    .distinct()

  tmpRes.persist()

  val upsellAbonents = tmpRes
    .select("ctn", "ban").distinct()
    .withColumn("rn", row_number().over(Window.orderBy(rand())))

  upsellAbonents.persist()

  val c = upsellAbonents.count()
  val bucketUpsell = upsellAbonents
    .withColumn("rr", when(col("rn") / c <= 0.1, 1)
    .otherwise(when(col("rn") / c > 0.1 && col("rn") / c <= 0.55,  2).otherwise(3)))
  val randdf = bucketUpsell.filter(col("rr") === 1)
  val maxUp = bucketUpsell.filter(col("rr") === 2)
  val maxScore = bucketUpsell.filter(col("rr") === 3)

  val randRes = {
    val df = tmpRes.join(randdf, Seq("ctn", "ban"), "inner")
    .withColumn("rand_rn", row_number().over(Window.partitionBy("ctn", "ban").orderBy(rand())))
    .filter(col("rand_rn") === 1)
    .drop("rn", "rr", "rand_rn")
    .distinct()
    .withColumn("fl_group", lit("rand"))
    df
}

  val maxUpRes ={
    val df = tmpRes.join(maxUp, Seq("ctn", "ban"), "inner")
      .withColumn("rand_rn", row_number().over(Window.partitionBy("ctn", "ban").orderBy(col("monthly_fee").desc, rand())))
      .filter(col("rand_rn") === 1)
      .drop("rn", "rr", "rand_rn")
      .distinct()
      .withColumn("fl_group", lit("max_up"))
      df
  }


 val maxScoreTmp = tmpRes
    .join(maxScore, Seq("ctn", "ban"), "inner")
    .withColumn("score",
      col("voice_traffic") / (col("avg_voice_traffic") + 0.00000001) +
        col("data_traffic") / (col("avg_data_traffic") + 0.00000001) +
        col("voice_traffic") / (col("curr_voice_fill") + 0.00000001) +
        col("data_traffic") / (col("curr_data_fill") + 0.00000001))


  val maxScoreRes = {
    val df = maxScoreTmp
    .withColumn("rand_rn", row_number().over(Window.partitionBy("ctn", "ban").orderBy(col("score").desc, rand())))
    .filter(col("rand_rn") === 1)
      .drop("rn", "rr", "rand_rn", "score")
      .distinct()
    .withColumn("fl_group", lit("max_score"))
    df
  }

  val res = randRes.unionByName(maxUpRes).unionByName(maxScoreRes)
    .select(
      col("ctn"),
      col("ban"),
      col("market"),
      col("arpau").as("arpau_base"),
      lit(null).as("segment"),
      col("curr_price_plan_key"),
      col("curr_voice_fill"),
      col("curr_data_fill"),
      col("avg_voice_traffic"),
      col("avg_data_traffic"),
      col("constructor_id").as("preset_id"),
      col("monthly_fee"),
      col("voice_traffic").as("preset_voice_traffic"),
      col("data_traffic").as("preset_data_traffic"),
      col("fl_group").as("group_name"),
      lit(LOAD_DATE_YYYY_MM_DD_1D).as("time_key")
    )
}
