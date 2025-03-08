package ru.beeline.cvm.commons.logging

import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, max}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ru.beeline.dmp.clientvictoriametrics.commons.utils.Common.cond

case class WorkerLoggingTable(spark: SparkSession,
                              techTableName: String,
                              techTablePath: String) extends Logging {

  import spark.implicits._

  log.info(s"Config for WorkerLoggingTable:")
  log.info(s"Table name with log info - ${techTableName}")
  log.info(s"Path for table with log info - ${techTablePath}")

  def addFilter(df: DataFrame, lstCondition: List[(String, String)]): DataFrame = {
    lstCondition match {
      case Nil => df
      case head :: tail => addFilter(df.filter(col(head._1) === head._2), tail)
    }
  }

  def readMetricFromTable(tableTrgName: String,
                          tableSrcName: String = null,
                          nameMetric: String): Map[String, Long] = {

    if (spark.catalog.tableExists(techTableName)) {

      val lstCondition: List[(String, String)] =
        List("table_trg" -> tableTrgName, "metric" -> nameMetric) :::
          cond(tableSrcName != null, "table_src" -> tableSrcName)

      val techTable = spark.table(techTableName)
      val techTableWithFilter = addFilter(techTable, lstCondition)

      val windowSpec = Window.partitionBy($"partition_processed").orderBy($"value_ts".desc)

      techTableWithFilter.
        withColumn("row_number", row_number.over(windowSpec)).
        filter($"row_number" === 1).
        select($"partition_processed", $"value_ts").
        collect().
        map(row => row.getString(0) -> row.getLong(1)).
        toMap
    }
    else {
      Map[String, Long]()
    }
  }

  def writeMetricToTable(values: Map[String, Long],
                         tableTrgName: String,
                         tableSrcName: String = null,
                         nameMetric: String): Unit = {

    log.info(s"Records for tech table:")

    values.map { value =>

      val rec = RecordLoggingTable(table_trg = tableTrgName,
        metric = nameMetric, table_src = tableSrcName,
        partition_processed = value._1, value_ts = value._2)

      log.info(s"metric: ${if (rec.metric != null) rec.metric else "null"}, " +
        s"table_trg: ${if (rec.table_trg != null) rec.table_trg else "null"}, " +
        s"table_src: ${if (rec.table_src != null) rec.table_src else "null"}, " +
        s"partition_processed: ${if (rec.partition_processed != null) rec.partition_processed else ""} -> " +
        s"value_ts: ${if (rec.value_ts != null) rec.value_ts else "0L"} ")

      rec
    }.
      toList.
      toDF().
      coalesce(1).
      write.
      partitionBy("table_trg", "table_src").
      mode(SaveMode.Append).
      format("parquet").
      option("path", techTablePath).
      saveAsTable(techTableName)
  }

  def writeMetricToTableLz(values: Map[String, Long],
                           tableTrgName: String,
                           tableSrcName: String = null,
                           nameMetric: String,
                           loadLz: Long): Unit = {
    log.info(s"Records for tech table:")

    values.map { value =>
      val rec = RecordLoggingTableLz(table_trg = tableTrgName,
        metric = nameMetric, table_src = tableSrcName, load_lz = loadLz,
        partition_processed = value._1, value_ts = value._2)


      log.info(s"metric: ${if (rec.metric != null) rec.metric else "null"}, " +
        s"table_trg: ${if (rec.table_trg != null) rec.table_trg else "null"}, " +
        s"table_src: ${if (rec.table_src != null) rec.table_src else "null"}, " +
        s"load_lz: ${if (rec.load_lz != null) rec.load_lz else "null"}, " +
        s"partition_processed: ${if (rec.partition_processed != null) rec.partition_processed else ""} -> " +
        s"value_ts: ${if (rec.value_ts != null) rec.value_ts else "0L"} ")

      rec
    }.
      toList.
      toDF().
      coalesce(1).
      write.
      partitionBy("table_trg", "table_src").
      mode(SaveMode.Append).
      format("parquet").
      option("path", techTablePath).
      saveAsTable(techTableName)
  }

  def getUpdMapMetric(lastMap: Map[String, Long],
                      previousMap: Map[String, Long]): Map[String, Long] = {
    lastMap.
      filter(el => !(previousMap.get(el._1).nonEmpty &&
        previousMap.get(el._1).get == el._2))
  }

  def getUpdListMetricWithLoadLzGroup(table_trg: String, partitions: List[String]): List[(Long, List[String])] = {

    if (spark.catalog.tableExists(techTableName)) {
      spark.table(techTableName).
        filter($"table_trg" === table_trg
          && $"metric" === "processed_trg"
          && $"partition_processed".isin(partitions: _*)).
        groupBy($"partition_processed").
        agg(max($"load_lz").as("load_lz")).
        select($"load_lz", $"partition_processed").
        collect().
        map(row => row.getLong(0) -> row.getString(1)).toList.
        groupBy(_._1).
        map(x => (x._1, x._2.map(y => y._2).sorted)).toList.
        sortBy(x => x._2.last)
    }
    else {
      List[(Long, List[String])]()
    }
  }

  def getUpdListMetricWithLoadLz(table_trg: String, partitions: List[String]): List[(Long, String)] = {

    if (spark.catalog.tableExists(techTableName)) {
      spark.table(techTableName).
        filter($"table_trg" === table_trg
          && $"metric" === "processed_trg"
          && $"partition_processed".isin(partitions: _*)).
        groupBy($"partition_processed").
        agg(max($"load_lz").as("load_lz")).
        select($"load_lz", $"partition_processed").
        collect().
        map(row => row.getLong(0) -> row.getString(1)).toList
    }
    else {
      List[(Long,String)]()
    }
  }
}
