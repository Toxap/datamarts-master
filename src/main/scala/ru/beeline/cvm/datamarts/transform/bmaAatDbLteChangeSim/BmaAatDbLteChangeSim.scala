package ru.beeline.cvm.datamarts.transform.bmaAatDbLteChangeSim
import org.apache.spark.sql.functions._
class BmaAatDbLteChangeSim extends Load {

  val tCampss = tCamps;
  val cmsMembers = cmsMemberss;
  val aggTimeSpendPosition = aggTimeSpendPositionn;
  val dimUcnNew = dimUcnNeww
  val lteAfter = lteAfterr
  val device = devicee
  val dataTraffic = dataTrafic
  val refarmLogin = loginRefarmm



    val campids = Seq(
        "1348412", "1345187", "1344467", "1343778", "1343206", "1343091", "1342915", "1342579", "1342343",
        "1341985", "1341368", "1341160", "1341134", "1339878", "1339721", "1339712", "1339699", "1339673",
        "1339073", "1338807", "1338795", "1337148", "1336636", "1336173", "1335677", "1335535", "1335518",
        "1335324", "1334783", "1334781", "1334735", "1334230", "1333569", "1333490", "1333306", "1333197",
        "1332772", "1332698", "1332644", "1332447", "1332231", "1331638", "1331535", "1330820", "1330817",
        "1329542", "1329319", "1329246", "1329230", "1328986", "1328884", "1328837", "1328510", "1327868",
        "1327557", "1327415", "1326961", "1326459", "1326027", "1325987", "1325752", "1325521", "1325458",
        "1325096", "1325084", "1325013", "1324915", "1324849", "1324693", "1317158", "1317156", "1316639",
        "1316180", "1315777", "1315734", "1315608", "1315595", "1315530", "1315351", "1315147", "1314947",
        "1314661", "1314569", "1314459", "1313903", "1313769", "1313464", "1312991", "1312831", "1312552",
        "1312435", "1312163", "1312129", "1311724", "1311372", "1311073", "1310523", "1310390", "1310385",
        "1310053", "1309954", "1309610", "1308627", "1308103", "1307735", "1307666", "1307468", "1307201",
        "1306955", "1306813", "1306761", "1306538", "1306491", "1306470", "1306271", "1306242", "1306211",
        "1306107", "1305802", "1305757", "1305705", "1304889", "1303845", "1303605", "1303524", "1303213",
        "1297028", "1296583", "1296490", "1296121", "1296026", "1295712", "1295669", "1295259", "1295237",
        "1294468", "1294399", "1294165", "1293757", "1293741", "1293557", "1293412", "1293368", "1293019",
        "1292581", "1292495", "1292414", "1292321", "1291692", "1291139", "1290971", "1290917", "1290644",
        "1290572", "1290087", "1289499", "1289345", "1288889", "1288694", "1288342", "1288155", "1287773",
        "1286715", "1286626", "1286545", "1286536", "1286464", "1286096", "1285321", "1284747", "1284510",
        "1284474", "1284409", "1284300", "1284230", "1283632", "1283178", "1281826", "1281774", "1281503",
        "1281309", "1279989", "1278773", "1278069", "1278006", "1277970", "1277969", "1277760", "1277706",
        "1276751", "1276610", "1276426", "1276240", "1275513", "1275469", "1275091", "1274298", "1274179",
        "1273454", "1273353", "1272904", "1272891", "1270675", "1270500", "1270007", "1269898", "1269605",
        "1269077", "1268932", "1268120", "1268033", "1267024", "1266845", "1266693", "1266356", "1266299",
        "1266079", "1265490", "1265409", "1263799", "1262764", "1262017", "1261781", "1261441", "1260558",
        "1259901", "1259864", "1258883", "1258357", "1258185", "1258149", "1258025", "1257731", "1257571",
        "1257396", "1257332", "1257143", "1256254", "1256238", "1254131", "1254092", "1254089", "1254086",
        "1254073", "1254072", "1254059", "1254055", "1253781", "1253713", "1253431", "1253172", "1252393",
        "1252071", "1251848", "1251263", "1251214", "1251166", "1250766", "1250283", "1250011", "1247772",
        "1245842", "1245293", "1244002", "1243819", "1243464", "1243432", "1242882", "1242701", "1242489",
        "1242276", "1241406", "1240464", "1240348", "1240234", "1239569", "1349591", "1349895"
    );

    val bmaAatChangeSimCampAll = tCampss
      .filter(col("wave_month") >= "2023-01-01")
      .filter(lower(col("camp_name_short")).contains("eff_sim") || col("camp_id").isin(campids: _*))
      .select("camp_id", "wave_month", "camp_name_short");



  val simMembers = cmsMembers
    .filter(col("time_key") >= "202301")
    .join(bmaAatChangeSimCampAll, cmsMembers("camp_id") === bmaAatChangeSimCampAll("camp_id"))
    .select(
      cmsMembers("subs_key"),
      cmsMembers("ban_key"),
      cmsMembers("market_key"),
      cmsMembers("member_group_type"),
      cmsMembers("real_taker_ind"),
      cmsMembers("response_date"),
      cmsMembers("sms_status_date"),
      cmsMembers("first_time_key"),
      cmsMembers("camp_id"),
      bmaAatChangeSimCampAll("camp_name_short"),
      bmaAatChangeSimCampAll("wave_month"));


  val maxDateRow = lteAfter
    .agg(max("time_key"))
    .collect()(0)

  val maxDate = maxDateRow
    .getAs[String]("max(time_key)")

  val lteAfterFiltered = lteAfter
    .filter(col("time_key") === maxDate);
  lteAfterFiltered.createOrReplaceTempView("lte_after")

  val maxDateDevice = device
    .agg(max("time_key"))
    .collect()(0)

  val maxDateD = maxDateDevice
    .getAs[String]("max(time_key)");

  val deviceFiltered = device.filter(col("time_key") === maxDateD);
  deviceFiltered.createOrReplaceTempView("device")


  val dataTrafficTransformed = dataTraffic
    .withColumn("time_key_data", concat(lit("P"), substring(col("transaction_dt"), 1, 4), substring(col("transaction_dt"), 6, 2)))
    .withColumn("flg_data_traffic", when(col("data_traffic_all") === 0, lit(0)).otherwise(lit(1)))
    .select(
      col("subscriber_num"),
      col("ban_num"),
      col("time_key_data"),
      col("flg_data_traffic"))


    val campIds2 = Seq(
        "1344467", "1348412", "1349591", "1349895", "1354672", "1357883", "1358402",
        "1363454", "1364050", "1368242", "1368754", "1377190", "1377493")


    val breakCamp1 = simMembers
      .filter(col("camp_id").isin(campIds2: _*))
      .join(lteAfter, simMembers("subs_key") === lteAfter("ctn") && date_add(to_date(simMembers("response_date")), -1) === lteAfter("time_key"), "left")
      .filter(lteAfter("time_key") >= "2023-11-01")
      .select(
          simMembers("camp_id").cast("int").as("camp_id"),
          simMembers("subs_key"),
          simMembers("ban_key"),
          to_date(simMembers("response_date")).as("tdate"),
          simMembers("real_taker_ind"),
          simMembers("MEMBER_GROUP_TYPE"),
          when(simMembers("subs_key") === lteAfter("ctn"), lteAfter("sim_lte")).otherwise(0).as("sim_lte_before"),
          lteAfter("time_key"))

  val breakCamp = breakCamp1
    .filter(col("sim_lte_before") !== 1)
    .select(
      col("camp_id"),
      col("subs_key"),
      col("ban_key"),
      col("member_group_type"),
      col("real_taker_ind"))

  val newRefarmLogin = refarmLogin
    .withColumn("flg_refarm",lit(1))
    .select(
      col("subs_key"),
      col("time_key"),
      col("flg_refarm"))
    .dropDuplicates()

    val campIds3 = Seq(
        "1344467",
        "1341134",
        "1339699",
        "1348412",
        "1349591",
        "1349895",
        "1354672",
        "1357883",
        "1358402",
        "1363454",
        "1364050",
        "1368242",
        "1368754",
        "1377190",
        "1377493")

    val step1 = simMembers
      .filter(!col("camp_id").isin(campIds3: _*))
      .select("camp_id", "subs_key", "ban_key", "member_group_type", "real_taker_ind")
      .union(breakCamp)
      .join(tCampss, Seq("camp_id"), "left")
      .withColumn("time_key", concat(lit("P"), substring(col("wave_month"), 1, 4), substring(col("wave_month"), 6, 2)))
      .join(lteAfterFiltered.select(col("ctn").alias("subs_key"), col("sim_lte")), Seq("subs_key"), "left")
      .join(deviceFiltered.select(col("ctn").alias("subs_key"), col("device_lte")), Seq("subs_key"), "left")
      .join(newRefarmLogin, Seq("subs_key", "time_key"), "left")
      .withColumn("flg_refarm_zone", coalesce(col("flg_refarm"), lit(0)))
      .join(dataTrafficTransformed.select(col("subscriber_num").alias("subs_key"), col("time_key_data").alias("time_key"), col("flg_data_traffic")), Seq("subs_key", "time_key"), "left")
      .withColumn("flg_data_traffic", coalesce(col("flg_data_traffic"), lit(0)))
      .filter(!col("time_key").isNull)
      .select(
          col("camp_id"),
          col("subs_key"),
          col("ban_key"),
          col("member_group_type"),
          col("real_taker_ind"),
        tCampss("camp_name_short"),
          col("info_channel_name"),
          coalesce(col("sim_lte"), lit(0)).alias("sim_lte"),
          coalesce(col("device_lte"), lit("0")).alias("device_lte"),
          coalesce(col("flg_data_traffic"), lit("0")).alias("flg_data_traffic"),
          col("flg_refarm_zone"),
          col("time_key"),
        tCampss("wave_month"),
          col("camp_desc"),
          col("use_case_new"),
          col("use_case_ext_new"))
}
