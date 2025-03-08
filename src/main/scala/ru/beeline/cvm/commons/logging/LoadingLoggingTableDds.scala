package ru.beeline.cvm.commons.logging

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import ru.beeline.cvm.commons.EntryPoint

object LoadingLoggingTableDds extends EntryPoint {

  val table_lz = List(
    ("nba_lz.agg_subs_usage_v3",
      Array("nba_dds.data_all_pub", "nba_dds.voice_all_pub", "nba_dds.charges_all_pub", "nba_dds.comm_arpu_all_pub")),
    ("nba_lz.fct_prep_charges_n", Array("nba_dds.charges_all_pub", "nba_dds.comm_arpu_all_pub")),
    ("nba_lz.fct_rtc_monthly", Array("nba_dds.paid_all_pub")),
    ("nba_lz.stg_agg_arpu_mou", Array("nba_dds.total_arpu_pub")),
    ("nba_lz.stg_badcell_daily", Array("nba_dds.badcell_pub")),
    ("nba_lz.stg_client_state", Array("nba_dds.block_count_pub")),
    ("nba_lz.fct_prep_recharges_n", Array("nba_dds.recharges_pub")),
    ("nba_lz.stg_device_changed", Array("nba_dds.last_device_pub")))

  import spark.implicits._

  val windowSpec = Window.partitionBy($"partition_processed").orderBy($"value_ts".desc)

  val lz = spark.table("nba_lz.logging_table")
    .filter($"table_trg".isin(table_lz.map(_._1): _*) && $"metric" === "processed_trg" && $"value_ts" < 1636068239000L)
    .withColumn("row_number", row_number.over(windowSpec))
    .filter($"row_number" === 1)
    .withColumn("metric", lit("processed_src"))
    .withColumn("table_src", $"table_trg")
    .withColumn("table_trg", table_lz.foldLeft(lit(null))((x, y) => when($"table_trg" === y._1, y._2).otherwise(x)))
    .select($"metric",
      $"partition_processed",
      $"value_ts",
      explode($"table_trg").as("table_trg"),
      $"table_src")


  val fs = FileSystem.get(new Configuration())

  case class TablesInfo(Path: String, TableTrg: String, TableSrc: String, Metric: String)

  val tablePathNName =
    List(
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/data_all_pub",
        "nba_dds.data_all_pub",
        null,
        "processed_trg"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/voice_all_pub",
        "nba_dds.voice_all_pub",
        null,
        "processed_trg"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/customers_pub",
        "nba_dds.customers_pub",
        null,
        "processed_trg"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/customers_pub",
        "nba_dds.customers_pub",
        "nba_dds.voice_all_pub",
        "processed_src"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/customers_pub",
        "nba_dds.customers_pub",
        "nba_dds.data_all_pub",
        "processed_src"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/call_to_callcenter_pub",
        "nba_dds.call_to_callcenter_pub",
        "nba_dds.customers_pub",
        "processed_src"
      ),
      TablesInfo(
        "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/last_price_plan_pub",
        "nba_dds.last_price_plan_pub",
        "nba_dds.customers_pub",
        "processed_src")
    )

  val dds = tablePathNName.map(x => {
    val partitions = fs.listStatus(new Path(x.Path)).map(x => x.getPath.getName.split("/").reverse.head).toList.tail

    val df = partitions.toDF("partition_processed")
      .withColumn("metric", lit(x.Metric))
      .withColumn("value_ts", lit(1636068239000L))
      .withColumn("table_trg", lit(x.TableTrg))
      .withColumn("table_src", lit(x.TableSrc))
      .select($"metric",
        $"partition_processed",
        $"value_ts",
        $"table_trg",
        $"table_src")

    df
  }).reduce(_.union(_))

  lz.union(dds).
    coalesce(1).
    write.
    partitionBy("table_trg", "table_src").
    mode(SaveMode.Append).
    format("parquet").
    option("path", "hdfs://ns-etl/warehouse/tablespace/external/hive/nba_dds.db/logging_table").
    saveAsTable("nba_dds.logging_table")

}
