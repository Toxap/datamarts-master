package ru.beeline.cvm.datamarts.transform.callCycles

import org.apache.spark.sql.functions._


object smCsBase2 extends Load {


  val dimCtnBan = getDimCtnBan
  val outbCrmBase = outbCrmBase1

  val b2cSegment = Seq("EXS", "N01", "N02", "N03", "N04", "N05", "N06", "N07", "N08", "N09", "N10", "N11", "N12");

  val a = outbCrmBase
    .withColumn("camp_type",
      when(lower(col("vip_title")).like("%prevention%"), "M. Prevention")
        .when(lower(col("vip_title")).like("%best tariff offer%"), "A. Best Tariff Offer")
        .when(lower(col("vip_title")).like("%overspend voice%"), "B. Overspend Voice")
        .when(lower(col("vip_title")).like("%overspend data%"), "B. Overspend Data")
        .when(lower(col("vip_title")).like("%overspend mg%"), "B. Overspend MG")
        .when(lower(col("vip_title")).like("%port out%"), "C. MNP Port Out")
        .when(lower(col("vip_title")).like("%single use%"), "D. Single Use")
        .when(lower(col("vip_title")).like("%ross-sell%"), "E. X-Sell")
        .when(lower(col("vip_title")).like("%flat2bundle%"), "F. Flat2Bundle")
        .when(lower(col("vip_title")).like("%up-sell%"), "G. Up-sell")
        .when(lower(col("vip_title")).like("%upsell%"), "G. Up-sell")
        .when(lower(col("vip_title")).like("%how are you%"), "H. How Are You")
        .when(lower(col("vip_title")).like("%ms&le%"), "I. MS&LE")
        .when(lower(col("vip_title")).like("%retention%"), "J. Retention")
        .when(lower(col("vip_title")).like("%reactivation%"), "K. Reactivation")
        .when(lower(col("vip_title")).like("%technica%"), "L. Technica")
        .when(lower(col("vip_title")).like("%information%"), "N. Information")
        .when(lower(col("vip_title")).like("%down-sell%"), "O. Down-sell")
        .when(lower(col("vip_title")).like("%win back%"), "P. Win Back")
        .when(lower(col("vip_title")).like("% LE %"), "Q. LE")
        .when(lower(col("vip_title")).like("%test%"), "R. Test")
        .otherwise("S. Other"))
    .withColumn("camp_FMC_flg",
      when(lower(col("vip_title")).like("%fttb%") ||
        lower(col("vip_title")).like("%fmc%") ||
        lower(col("vip_title")).like("%convergence%"), "Yes")
        .otherwise("No"))
    .select(col("subs_key"),
      col("ban_key"),
      col("creation_time").as("time_key"),
      col("time_key_src"),
      col("start_date").as("call_start_time"),
      col("end_date").as("call_end_time"),
      col("conversation_duration").as("call_duration"),
      col("interaction_reason_level_4").as("call_result"),
      col("vip_title").as("camp_nm_full"),
      col("camp_type"),
      col("camp_FMC_flg"));


  val b = dimCtnBan

  val tableResult = a
    .join(b, Seq("subs_key", "ban_key", "time_key_src"), "left")


  tableResult
    .write
    .mode("overwrite")
    .format("orc")
    .partitionBy("time_key")
    .saveAsTable(TableSmCsBase2);


}
