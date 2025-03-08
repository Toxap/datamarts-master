package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import scala.util.matching.Regex

object outbCrmBase extends Load {

  val callsOutBound = callOutBound;
  val reason = reason1;
  val ban = getBan;

  val outboundCrm = callsOutBound
    .filter(lower(col("type")) === "звонок"
      and col("direction") === 2
      and to_date(col("start_time")) >= "2022-01-01")
    .withColumn("time_key_src", concat(to_date(col("start_time")).substr(0, 8), lit("01")))
    .select(
      col("x_calling_phone").as("subs_key"),
      col("x_intrxn2fin_accnt"),
      col("objid").as("topic2intrxn"),
      col("start_time").as("start_date"),
      col("end_time").as("end_date"),
      (col("end_time").cast("long") - col("start_time").cast("long")).as("conversation_duration"),
      col("title").as("vip_title"),
      col("time_key_src")
    )
    .filter(col("conversation_duration") > 2)

  val directoryOfCauses = reason
    .select(
      col("topic2intrxn"),
      col("reason_1"),
      col("reason_2"),
      col("result")
    )
    .filter(
      !col("result").isNaN
        and lower(col("result")) =!= "none"
        and col("result") =!= ""
        and col("result").isNotNull)
    .dropDuplicates("topic2intrxn");

  val banDF = ban
    .select(
      col("objid").as("x_intrxn2fin_accnt"),
      col("s_fa_id").as("ban_key")
    )

  val outbCrm = outboundCrm
    .join(banDF, Seq("x_intrxn2fin_accnt"), "left")
    .join(directoryOfCauses, Seq("topic2intrxn"), "left")
    .select(
      col("subs_key"),
      col("ban_key"),
      col("vip_title"),
      col("result").as("interaction_reason_level_4"),
      col("conversation_duration"),
      col("start_date"),
      col("end_date"),
      concat(lit("P"), col("time_key_src").substr(1, 4), col("time_key_src").substr(6, 2)).as("creation_time"),
      col("time_key_src")
    )

  val mainPattern = "^[А-Яа-яA-Za-z]{8}\\s\\d{7}".r.pattern.pattern();
  val digitPattern = "\\d{7}";

  val outbCrmWithId = outbCrm.filter(regexp_extract(col("vip_title"), mainPattern, 0) =!= "")
    .withColumn("crm_camp_id", regexp_extract(col("vip_title"), digitPattern, 0));

  val outbCrmB2cReal = outbCrmWithId
    .filter(col("crm_camp_id").isNotNull)
    .withColumn("crm_camp_id", col("crm_camp_id").cast(IntegerType))

  outbCrmB2cReal
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key_src")
    .saveAsTable(TableOutbCrmBase202101V2);

}
