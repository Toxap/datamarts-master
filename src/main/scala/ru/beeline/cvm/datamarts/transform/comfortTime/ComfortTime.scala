package ru.beeline.cvm.datamarts.transform.comfortTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


class ComfortTime extends Load {

  val lastPart = getLastPartFromAggSubs;
  val aggSubsProfile = getAggSubsProfile(lastPart);
  val stgGeoAll = getStgGeoAll;
  val trefMarketInfo = getTrefMarketInfo;
  val nbaCtnTzPrev = getNbaCtnTzPrev;

  val nbaCtnLastEvent7d = stgGeoAll
    .groupBy("ctn")
    .agg(max(col("event_time_msk")).as("last_event"))
    .select(
      col("ctn"),
      col("last_event")
    );

  val nbaCtnLastEvent7dExt = nbaCtnLastEvent7d
    .filter(length(col("ctn")) === 10)
    .join(stgGeoAll, Seq("ctn"))
    .filter(col("last_event") === col("event_time_msk"))
    .groupBy("ctn", "last_event")
    .agg(min(col("timezone")).as("tz"))
    .select(
      col("ctn"),
      col("last_event"),
      col("tz")
    );


  val nbaCtnTz = aggSubsProfile
    .join(nbaCtnLastEvent7dExt, aggSubsProfile("subs_key") === nbaCtnLastEvent7dExt("ctn"), "outer")
    .join(trefMarketInfo, aggSubsProfile("market_key") === trefMarketInfo("market_cd"), "left")
    .join(nbaCtnTzPrev
      .filter(col("weeks_ago") < 6)
      .filter(col("weeks_ago") > -1), aggSubsProfile("subs_key") === nbaCtnTzPrev("ctn"), "left")
    .select(
      when(aggSubsProfile("subs_key").isNotNull, aggSubsProfile("subs_key")).otherwise(nbaCtnLastEvent7dExt("ctn")).as("subs_key"),
      aggSubsProfile("market_key"),
      coalesce(
        coalesce(nbaCtnLastEvent7dExt("tz"), nbaCtnTzPrev("tz")), trefMarketInfo("time_zone_msk") + 3).as("tz"),
      (trefMarketInfo("time_zone_msk") + 3).as("market_tz"),
      (when(coalesce(coalesce(nbaCtnLastEvent7dExt("tz"), nbaCtnTzPrev("tz")), lit(0)) === 0, 1)
        .otherwise(0)).as("used_market_tz"),
      (when(coalesce(coalesce(nbaCtnLastEvent7dExt("tz"), nbaCtnTzPrev("tz")), lit(0)) === 0, lit(-1))
        .when(nbaCtnLastEvent7dExt("tz").isNotNull, lit(0))
        .otherwise(nbaCtnTzPrev("weeks_ago"))).as("weeks_ago")
    );

  val nbaCtnTzOnlyChanges = nbaCtnTz.as("new")
    .join(nbaCtnTzPrev.as("prev"), nbaCtnTzPrev("ctn") === nbaCtnTz("subs_key"),"left")
    .filter(nbaCtnTzPrev("tz") =!= nbaCtnTz("tz"))
    .select(
      col("subs_key"),
      col("new.market_key"),
      col("new.weeks_ago"),
      col("new.tz"),
      col("used_market_tz"));


  val resultDf = nbaCtnTz
    .withColumn("startComfortTime", concat(lit("09:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
      .otherwise(col("tz"))))
    .withColumn("endComfortTime", concat(lit("22:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
      .otherwise(col("tz"))))
    .withColumn("timezoneMethod", when(col("used_market_tz")===0, lit("GEOZONE")).otherwise(lit("MARKET_CODE")))
    .select(
      col("subs_key").as("ctn"),
      col("startComfortTime"),
      col("endComfortTime"),
      col("timezoneMethod")
    );

  val resultDfOnlyChanges = nbaCtnTzOnlyChanges
    .withColumn("startComfortTime", concat(lit("09:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
      .otherwise(col("tz"))))
    .withColumn("endComfortTime", concat(lit("22:00:00+"), when(col("tz") < 10, concat(lit("0"), col("tz")))
      .otherwise(col("tz"))))
    .withColumn("timezoneMethod", when(col("used_market_tz")===0, lit("GEOZONE")).otherwise(lit("MARKET_CODE")))
    .select(
      col("subs_key").as("ctn"),
      col("startComfortTime"),
      col("endComfortTime"),
      col("timezoneMethod")
    );



}
