package ru.beeline.cvm.commons.filemerger

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

case class WorkerFileMerger(spark: SparkSession) extends Logging{

  val confFs = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(confFs)

  def merger(path: String, exclusion: Int, repartition: Int): AnyVal = {

    log.info(s"Parent path to merge: ${path}")
    val dir = new Path(path)

    val dirTmp = new Path(s"${path}_tmp")
    if (fs.exists(dirTmp)) fs.delete(dirTmp, true)
    fs.mkdirs(dirTmp)

    val paths = fs.listStatus(dir).
      filter(_.isDirectory).
      toList.
      filter(!_.getPath.toString.contains("/.")).
      sortBy(_.getPath.toString).
      dropRight(exclusion).
      map(_.getPath)

    paths.foreach(processingPathWithParquet)

    def processingPathWithParquet(path: Path): Unit = {
      val lstDirPath = fs.listStatus(path).filter(_.isDirectory)

      if (lstDirPath.size > 0) {
        lstDirPath.map(_.getPath).foreach(processingPathWithParquet)
      }
      else {
        log.info(s"Child path to merge: ${path.toString}")
        val newPath = new Path(s"${dirTmp.toString}/${path.toString.split("/").last}")
        spark.
          read.
          parquet(path.toString).
          repartition(repartition).
          write.
          mode(SaveMode.Overwrite).
          parquet(newPath.toString)

        if (fs.exists(path)) fs.delete(path, true)
        if (fs.exists(newPath) && !fs.exists(path)) fs.rename(newPath, path)
      }
    }

    if (fs.exists(dirTmp)) fs.delete(dirTmp, true)
  }
}
