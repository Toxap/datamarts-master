package ru.beeline.cvm.commons.parquetutils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode

object ModifyParquet extends CustomParams {

  val confFs = spark.sparkContext.hadoopConfiguration
  val fs = FileSystem.get(confFs)

  val lstPaths = spark.read.textFile(pathInfoFile).collect.toList
  lstPaths.foreach { pathTrgStr =>
    val pathSrcStr = s"${pathTrgStr}_new"

    val dfWithOldNaming = spark.read.parquet(pathTrgStr)

    val dfWithNewNaming = dfWithOldNaming.
      toDF(dfWithOldNaming.columns.map(_.toLowerCase): _*)

    dfWithNewNaming.
      coalesce(coalesce).
      write.
      mode(SaveMode.Overwrite).
      parquet(pathSrcStr)

    val pathTrg = new Path(pathTrgStr)
    val pathSrc = new Path(pathSrcStr)
    if (fs.exists(pathTrg)) fs.delete(pathTrg, true)

    if (fs.exists(pathSrc) && !fs.exists(pathTrg)) fs.rename(pathSrc, pathTrg)

  }

  spark.stop()
}
