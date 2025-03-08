package ru.beeline.cvm.datamarts.transform.coreClientsTarget

import org.apache.spark.sql.functions.{col, when}

object CoreClientsModelTarget extends CustomParams {
  val beem_sales = spark.read.table(tableDmMobileSubscriber)
    .withColumn("calendar_dt", col("calendar_dt").cast("string"))
    .where(col("calendar_dt") >= LOAD_DATE_YYYY_MM_DD_minus3 &&
      col("calendar_dt") < LOAD_DATE_YYYY_MM_DD_minus2 &&
      col("sales_to_flg") === 1 &&
      col("test_price_plan_flg") === 0 &&
      col("test_ban_flg") === 0 &&
      col("test_account_flg") === 0 &&
      col("segment_cd") =!= "-99" &&
      col("segment_cd") =!= "TST" &&
      col("business_type_cd") === "B2C")
    .select(
      col("subscriber_sk"),
      col("subscriber_num"),
      col("ban_num"),
      col("calendar_dt"),
      col("activation_dt")
    )

  val sample = spark.read.table(tableCoreModelScoring2)
    .filter(col("sale_dt") >= LOAD_DATE_YYYY_MM_DD_minus3 &&
      col("sale_dt") < LOAD_DATE_YYYY_MM_DD_minus2)
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("date_key")
    )

  val coreseg = spark.read.table(tableDmMobileSubscriberCoreSegment)
    .withColumn("transaction_dt", col("transaction_dt").cast("string"))
    .filter(col("transaction_dt") === LOAD_DATE_YYYY_MM_DD_0)
    .select(
      col("core"),
      col("core_segment_lvl1"),
      col("core_segment_lvl2"),
      col("core_segment_lvl3"),
      col("subscriber_sk"),
      col("transaction_dt")
    )

  val target = sample
    .join(beem_sales, sample("subs_key") === beem_sales("subscriber_num") &&
      sample("ban_key") === beem_sales("ban_num"), "left")
    .join(coreseg, beem_sales("subscriber_sk") === coreseg("subscriber_sk"), "left")
    .withColumn("partition_key", col("date_key"))
    .withColumn("flag_core_lvl1", when(col("core_segment_lvl1") === "Core", 1).otherwise(0))
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      col("sale_dt"),
      col("date_key"),
      col("dm_mobile_subscriber.subscriber_sk"),
      col("calendar_dt").cast("date"),
      col("activation_dt"),
      col("core"),
      col("core_segment_lvl1"),
      col("core_segment_lvl2"),
      col("core_segment_lvl3"),
      col("flag_core_lvl1"),
      col("partition_key")
    )

  target
    .repartition(1)
    .write
    .format("orc")
    .mode("overwrite")
    .insertInto("nba_engine.core_clients_model_target")

  spark.catalog.clearCache()
  spark.stop()
}
