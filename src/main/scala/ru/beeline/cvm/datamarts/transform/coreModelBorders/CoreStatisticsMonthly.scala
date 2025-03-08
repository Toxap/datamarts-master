package ru.beeline.cvm.datamarts.transform.coreModelBorders

import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.functions._

object CoreStatisticsMonthly extends CustomParams {

  val q_sdf = spark.table(TableCoreBordersMonthly)

  val getBucketBorder = udf((bucket: Double, labels: Seq[String]) => labels(bucket.toInt));

  val sdf = spark.table(TableCoreModelScoring)
    .select(
      trunc(to_date(col("sale_dt")), "month").as("sale_dt"),
      col("pred_calibrated"),
      col("subs_key")
    );

  val start_date = sdf
    .selectExpr("add_months(max(sale_dt), -8)")
    .first()
    .getDate(0);

  val sdf_filtered = sdf.filter(col("sale_dt") >= start_date);

  val states = q_sdf
    .select(col("q"))
    .rdd
    .flatMap(r => r.getAs[Seq[Double]](0))
    .collect();

  val borders = Array(0.0) ++ states ++ Array(1.0)
  .distinct;

  val bucketizer = new Bucketizer()
    .setSplits(borders)
    .setInputCol("pred_calibrated")
    .setOutputCol("buckets");

  val bucketed = bucketizer
    .transform(sdf_filtered);

  val labels = 0.0 +: states.map(i => math.round(i * 10000) / 10000.0) :+ 1.0;
  val new_labels = labels
    .sliding(2)
    .map(arr => s"(${arr.head}: ${arr.last}]")
    .toArray;


  val bucketedWithBorders = bucketed
    .withColumn("border", getBucketBorder(col("buckets"), lit(new_labels)))
    .groupBy("sale_dt", "border")
    .agg(count("subs_key").as("subs_key"))
    .select(
      col("border"),
      col("subs_key"),
      col("sale_dt")
    )

  bucketedWithBorders
    .repartition(1)
    .write
    .format("orc")
    .mode("overwrite")
    .insertInto(TableCoreStatisticsMonthly)

}
