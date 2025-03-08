package ru.beeline.cvm.datamarts.transform.prolongAggr

import org.apache.spark.sql.functions.{col, count, lit}

class prolongAggr extends Load {

  val ctnCount = getUniqueCtn;
  val requestIdCount = getUniqueRequestId;
  val prolongOutput = getProlongOutput;
  val prolongCustFeatures = getProlongCustFeatures;

  val prolongAggrFinalTable =
    prolongCustFeatures
      .join(prolongOutput, Seq("request_id"))
      .groupBy(prolongCustFeatures("time_key"), col("count_data_gb_offer"), col("offer_rank"))
      .agg(
        count(col("subs_key")).as("ctn_count"),
        count(col("request_id")).as("req_count")
      )
      .withColumn("unique_ctn_count", lit(ctnCount))
      .withColumn("unique_req_count", lit(requestIdCount))
      .select(
        col("count_data_gb_offer").cast("double"),
        col("offer_rank"),
        col("ctn_count"),
        col("req_count"),
        col("unique_ctn_count"),
        col("unique_req_count"),
        col("time_key")
      );

}
