package ru.beeline.cvm.datamarts.transform.bmaAatDbLteChangeSim

import org.apache.spark.sql.functions._
object AggTimeSpendPositionCodesPubD extends Load{

  val aggTimeSpendPosition = aggTimeSpendPositionn;
  val dimUcnNew = dimUcnNeww
  val newMonth = "2024-05-01";
  val nextMonth = "2024-06-01";


  val aggTimeSpendPositionCodesPubD = aggTimeSpendPosition
    .filter((col("time_key")>=newMonth) && (col("time_key")<nextMonth))
    .filter(col("total_time") > 0)
    .join(dimUcnNew,Seq("position_code"),"left_semi")
    .withColumn("time_key_n",concat(col("time_key").substr(1,4), col("time_key").substr(6,2)))
    .select(
      col("ctn").alias("subs_key"),
      col("ban").alias("ban_key"),
      col("position_code"),
      col("time_key_n"))
    .dropDuplicates();

  val loginRefarm = aggTimeSpendPositionCodesPubD
    .withColumn("time_key",concat(lit("P"), col("time_key_n")))
    .withColumn("flg_refarm", lit(1))
    .select(
      col("flg_refarm"),
      col("subs_key"),
      col("ban_key"),
      col("time_key"))
    .dropDuplicates();



  loginRefarm
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key")
    .saveAsTable(TableBmaAatLoginRefarm);
}
