package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._

object smCsCyclesFull extends Load {

  val SmCsCycles2 = atsSmCsCycles2
  val SmCsCyclesPrev2 = atsSmCsCyclesPrev2
  val smCsCyclesNext = smCsCyclesNext1

  val fA = SmCsCycles2;

  val fB = SmCsCyclesPrev2;

  val fC = smCsCyclesNext;

  val fTableResult = fA
    .join(fB
      , Seq("subscriber_sk", "time_key_src", "cycle_start"), "left")
    .join(fC
      , Seq("subscriber_sk", "time_key", "cycle_start"), "left");

  fTableResult
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableSmCsCyclesFull);

}
