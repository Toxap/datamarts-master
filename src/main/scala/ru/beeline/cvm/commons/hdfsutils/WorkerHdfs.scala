package ru.beeline.cvm.commons.hdfsutils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

case class WorkerHdfs(spark: SparkSession) {
  val sc = spark.sparkContext

  val conf = sc.hadoopConfiguration
  val fs = FileSystem.get(conf)

  def getLastModifyPath(path: Path): Long = {
    val lstStatus = fs.
      listStatus(path).
      map(_.getModificationTime)
    if (lstStatus.nonEmpty) {
      lstStatus.max
    } else {
      fs.getFileStatus(path).getModificationTime
    }
  }

  def getLastModifyPath(name: String): Long = getLastModifyPath(new Path(name))

  def getMapWithLastModify(name: String, partitioned: Boolean = true): Map[String, Long] = {
    if (partitioned) {
      fs.
        listStatus(new Path(name)).
        filter(status => (status.isDirectory) &&
          (!status.getPath.getName.startsWith("."))).
        map(fileStatus => (fileStatus.getPath.getName,
          getLastModifyPath(fileStatus.getPath))).
        toMap
    } else {
      fs.
        listStatus(new Path(name)).
        filter(status => (status.isFile) &&
          (!status.getPath.getName.startsWith("_"))).
        map(fileStatus => ("update",
          getLastModifyPath(fileStatus.getPath))).
        toMap
    }
  }

  def renameFile(path: String, name: String): Boolean = {
    val oldPath = fs.
      listStatus(new Path(path)).
      filter(_.isFile).
      filter(_.getPath.getName.endsWith(".csv"))(0).getPath
    val newPath = new Path(path + "/" + name)

    if (fs.exists(newPath)) {
      fs.delete(newPath, false)
    }
    fs.rename(oldPath, newPath)
  }
}
