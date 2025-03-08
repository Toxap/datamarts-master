package ru.beeline.cvm.commons.metrics

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import ru.beeline.cvm.commons.hdfsutils.WorkerHdfs

@Deprecated
class WorkerWithMetrics(spark: SparkSession) {
  import spark.implicits._

  val sc = spark.sparkContext

  val conf = sc.hadoopConfiguration
  val fs = FileSystem.get(conf)

  val workerHdfs = WorkerHdfs(spark)

  def writeMetricsToCsvFormat(map: Map[String, Long],
                              path: String,
                              outFileName: String,
                              sep: String = ",",
                              header: Boolean = false
                             ): Unit = {

    map.
      toSeq.
      toDF("part", "value").
      coalesce(1).
      write.
      option("header", header.toString).
      option("delimiter", sep).
      mode(SaveMode.Append).
      csv(path)

    workerHdfs.renameFile(path, outFileName)
  }

  def readMetricsFromCsvFormat(path: String,
                               sep: String = ",",
                               header: Boolean = false): Map[String, Long] = {

    if (fs.exists(new Path(path))) {
      val schema = StructType(Seq(
        StructField("part", StringType, true),
        StructField("value", LongType, false)))

      val pathForRead = if (!new Path(path).getName.startsWith("_")) {
        path
      } else {
        val srcPath = new Path(path)
        val dstPath = new Path(sc.applicationId)

        if (fs.exists(dstPath)) fs.delete(dstPath, false)

        FileUtil.copy(fs, srcPath, fs, dstPath, false, conf)
        dstPath.toString()
      }

      val df = spark.
        read.
        option("header", header.toString).
        option("delimiter", sep).
        schema(schema).
        csv(pathForRead)

      val mapMetrics = df.
        collect().
        map(row => row.getAs[String]("part") ->
          row.getAs[Long]("value")).
        toMap
      if (path != pathForRead) {
        fs.delete(new Path(pathForRead), false)
      }

      mapMetrics
    }
    else {
      Map[String, Long]()
    }
  }

  def getUpdMetrics(lastMap: Map[String, Long],
                previousMap: Map[String, Long]): Map[String, Long] = {
    lastMap.
      filter(el => !(previousMap.get(el._1).nonEmpty &&
        previousMap.get(el._1).get == el._2))
  }
}
