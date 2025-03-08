package ru.beeline.cvm.commons.hiveutils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object CopyDataAndUpdateMetastore extends CustomParams {
  // Копирование данных из одной директорию в другую, и обновление метаданных в таблице.
  // Если надо перенести не все данные из директории, а только часть,
  // то задать конфиги firstPart, lastPart в формате имени партиции после "=", т.е. только дату.

  val conf = new Configuration()
  val fs = FileSystem.get(conf)

  val existsTargetPath = fs.exists(new Path(hdfsPathTarget.get))
  val existsSourcePath = fs.exists(new Path(hdfsPathSource.get))

  if (existsTargetPath && existsSourcePath) {
    val trgPath = new Path(hdfsPathTarget.get)
    val srcPath = new Path(hdfsPathSource.get)

    val listParts = fs.listStatus(srcPath)
      .map(x => x.getPath)
      .filter(x => {
        if (x.toString.endsWith("_SUCCESS") || x.toString.split("/").tail.reverse.head.startsWith(".")) {
          false
        } else if (firstPart.isDefined && lastPart.isDefined) {
          val part = x.toString.split("=").tail.head
          part >= firstPart.get && part <= lastPart.get
        } else {
          true
        }
      })

    for (part <- listParts) {
      org.apache.hadoop.fs.FileUtil.copy(part.getFileSystem(conf), part, trgPath.getFileSystem(conf),
        trgPath, false, conf)
    }

    val existsMetaForTable = spark
      .sharedState
      .externalCatalog
      .tableExists(tableName.get.split("\\.").head, tableName.get.split("\\.").tail.head)

    if (existsMetaForTable) spark.catalog.recoverPartitions(tableName.get)

    spark.stop()
  }
}
