package ru.beeline.cvm.datamarts.transform.upsell2plusDataset

import org.apache.spark.sql.DataFrame
import ru.beeline.cvm.datamarts.transform.upsell2plusDataset.Transformations._

object Upsell2PlusDataset extends Load {
  val subscribers: DataFrame = getSubscribers
  val subscriberHistory: DataFrame = getSubscriberHistory

  subscriberHistory.cache

  val recharges: DataFrame = aggregateRecharges(
    subscriberHistory,
    LOAD_DATE_PYYYYMM_2M
  )
  val subscriberHistoryMinusTwoMonth: DataFrame = getSubscriberHistoryMinusTwoMonth(
    subscriberHistory,
    LOAD_DATE_PYYYYMM_2M,
    LOAD_DATE_YYYY_MM_01
  )

  subscriberHistory.unpersist()

  val subscriberHistoryWithRecharges: DataFrame = getSubscriberHistoryWithRecharges(
    subscriberHistoryMinusTwoMonth,
    recharges
  )
  val bans: DataFrame = getBans
  val tariffs: DataFrame = getTariffs
  val subscriberArpu: DataFrame = getSubscriberArpu

  val subscriberArpuAggregate = aggregateSubscriberArpu(subscriberArpu)

  val bansCleanCustomerAge: DataFrame = cleanCustomerAge(bans)

  val upsellSample: DataFrame = getUpsellSample(
    subscribers,
    subscriberHistoryWithRecharges,
    bansCleanCustomerAge,
    subscriberArpuAggregate,
    tariffs,
    LOAD_DATE_PYYYYMM
  )

  upsellSample.repartition(NUM_PARTITIONS)
    .write
    .format("orc")
    .mode("overwrite")
    .insertInto(tableUpsellDataset)
}
