package ru.beeline.cvm.commons.hdfsutils

import org.apache.hadoop.fs.{FileSystem, Path}
import ru.beeline.cvm.commons.CustomParamsInit

trait utils extends CustomParamsInit {

  def getMaxTimeKey(tableName: String): String = {
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val tablePath = s"/warehouse/tablespace/external/hive/${tableName.replaceFirst("\\.", ".db/")}"
    val status = hdfs.listStatus(new Path(tablePath))

    val timeKeys = status.map { fileStatus =>
      val dirName = fileStatus.getPath.getName
      if (dirName.startsWith("report_dt=")) {
        dirName.split("=").last
      } else if (dirName.startsWith("time_key=")) {
        dirName.split("=").last
      } else {
        ""
      }
    }.filter(_.nonEmpty)

    val maxTimeKey = timeKeys.max
    maxTimeKey
  }

}
