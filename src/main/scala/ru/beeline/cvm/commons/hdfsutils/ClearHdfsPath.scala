package ru.beeline.cvm.commons.hdfsutils

import org.apache.hadoop.fs.{FileSystem, Path}

object ClearHdfsPath extends CustomParams {

  val confFs = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(confFs)

  val lstPaths = spark.read.textFile(pathInfoFile).collect.toList
  lstPaths.foreach { pathStr =>

    val path = new Path(pathStr)

    if (fs.exists(path)) fs.delete(path, recursiveClear)
  }

  spark.stop()
}
