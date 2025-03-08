package ru.beeline.cvm.datamarts.transform.coreModelBorders

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions._

object CoreBordersMonthly extends CustomParams {

  def percentileApprox(col: Column, percentage: Column, accuracy: Column): Column = {
    val expr = new ApproximatePercentile(
      col.expr, percentage.expr, accuracy.expr).toAggregateExpression
    new Column(expr)
  }

  val core_model = spark.read.table(TableCoreModelScoring)
    .select(
      col("sale_dt"),
      col("pred_calibrated")
    )
    .withColumn("sale_dt", trunc(to_date(col("sale_dt")), "month"));

  val max_date = spark.read.table(TableCoreModelScoring)
    .select(max("sale_dt").as("sale_dt"))
    .withColumn("sale_dt", add_months(trunc(col("sale_dt"), "month"), -9));

  val reference_month = max_date.join(core_model, "sale_dt");

  val q_sdf = reference_month
    .repartition(1)
    .groupBy("sale_dt")
    .agg(percentileApprox(col("pred_calibrated"),
      typedLit(Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)), lit(10000)).as("q"))
    .withColumn("q", col("q").cast("array<double>"));

  q_sdf
    .repartition(1)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableCoreBordersMonthly)

}
