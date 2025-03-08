package ru.beeline.cvm.datamarts.transform.antidownsellDataset

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class DimAllSoc extends Load {

  private val socPresetShortList = List("P1D", "P1V", "P2D", "P2V", "P3D", "P3V", "P4D", "P4V", "P5D", "P5V", "P6D", "P6V")
  val listConstructorSoc = Seq("CXK21_1", "EXK21_SAT", "EXK21_1", "VXK21_1", "SXK21_1", "WXK21_1",
    "EXK21RSAT", "EXK21R", "WXK21R", "CXK21R", "SXK21R", "VXK21R",
    "VXK22", "VXK22_2", "VXK22_3", "VXK22_4", "VXK22_5",
    "WXK22", "WXK22_2", "WXK22_3", "WXK22_4", "WXK22_5",
    "SXK22", "SXK22_2", "SXK22_3", "SXK22_4", "SXK22_5",
    "CXK22", "CXK22_2", "CXK22_3", "CXK22_4", "CXK22_5",
    "EXK22", "EXK22_2", "EXK22_3", "EXK22_4", "EXK22_5",
    "EXK22SAT", "EXK22SAT2", "EXK22SAT3", "EXK22SAT4", "EXK22SAT5",
    "CXK22R", "EXK22R", "SXK22R", "WXK22R",
    "VXK22R1", "VXK22R2", "VXK22R3", "VXK22R4", "VXK22R5", "WXK22R1",
    "WXK22R2", "WXK22R3", "WXK22R4", "WXK22R5", "SXK22R1", "SXK22R2",
    "SXK22R3", "SXK22R4", "SXK22R5", "CXK22R1", "CXK22R2", "CXK22R3",
    "CXK22R4", "CXK22R5", "EXK22R1", "EXK22R2", "EXK22R3", "EXK22R4",
    "EXK22R5",
    "CXK3R1", "CXK3R2","CXK3R3","CXK3R4","CXK3R5",
    "EXK3RSAT1","EXK3RSAT2","EXK3RSAT3","EXK3RSAT4","EXK3RSAT5",
    "EXK3R1","EXK3R2","EXK3R3","EXK3R4","EXK3R5",
    "WXK3R1","WXK3R2","WXK3R3","WXK3R4","WXK3R5",
    "SXK3R1","SXK3R2","SXK3R3","SXK3R4","SXK3R5",
    "VXK3R1","VXK3R2","VXK3R3","VXK3R4","VXK3R5")

  private val dimSocParameter = getDimSocParameter
  private val dimDic = getDimDic
  private val dimDicLink = getDimDicLink
  private val dimPpConstructorSoc = getDimPpConstructorSoc
  private val productInfo = createProductInfo
  private val serviceAgreement =  getServiceAgreementPub


  def createProductInfo: DataFrame = {

    val productNameAndCategory = dimDicLink.as("ddl")
      .join(dimDic.filter(col("parent_isn") === 12619).as("dc"), col("dc.isn") === col("ddl.dic2_isn"))
      .join(dimDic.filter(col("parent_isn") === 12618).as("dc1"), col("dc1.isn") === col("ddl.dic1_isn"))
      .select(col("dc.isn").as("product_id"),
              col("dc.name_eng").as("product_name"),
              col("dc1.name_eng").as("product_category"))
      .distinct()

    dimSocParameter
      .filter(col("soc_code").isNotNull && col("product_id").isNotNull)
      .select(col("soc_code"),
              col("product_id"),
              col("market_key"),
              col("business_name"))
      .distinct()
      .join(productNameAndCategory, Seq("product_id"), "left")
      .select(
        col("soc_code"),
        col("product_id").as("product_id_product_info"),
        col("market_key"),
        col("business_name").as("business_name_product_info"),
        col("product_name").as("product_name_product_info"),
        col("product_category").as("product_category_product_info")
      )
      .distinct()
  }

  def createConstructorSocContent: DataFrame = {

    dimPpConstructorSoc
      .join(productInfo, Seq("soc_code", "market_key"), "left")
      .groupBy(col("soc_code"), col("constructor_id"), col("market_key"),
        col("business_name_product_info"),
        col("product_id_product_info"), col("product_name_product_info"),
        col("product_category_product_info"), col("start_time_key"), col("end_time_key"), col("active_ind"))
      .agg(sum(col("long_rc")).as("monthly_fee"), sum(col("daily_rc")).as("daily_fee"),
        concat_ws(",", collect_list(when(col("class") === "V", col("wep_pkg_amount_long") / 60)
          .otherwise(null))).as("count_voice_min"),
        concat_ws(",", collect_list(when(col("class") === "G", col("wep_pkg_amount_long") / 1024 / 1024)
          .otherwise(null))).as("count_data_gb"),
        lit(0).as("count_sms"),
        lit(0).as("count_available_family_ctn"),
        concat_ws(",", collect_list(when(col("class") === "V", col("soc"))
          .otherwise(null))).as("voice_soc"),
        concat_ws(",", collect_list(when(col("class") === "G", col("soc"))
          .otherwise(null))).as("data_soc"))
      .select(
        col("soc_code"),
        col("constructor_id"),
        col("voice_soc"),
        col("data_soc"),
        col("market_key"),
        col("business_name_product_info"),
        col("product_id_product_info"),
        col("product_name_product_info"),
        col("product_category_product_info"),
        col("monthly_fee"),
        col("daily_fee"),
        col("count_voice_min"),
        col("count_data_gb"),
        col("count_sms"),
        col("count_available_family_ctn"),
        col("start_time_key"),
        col("end_time_key"),
        when(col("active_ind") === 1, 0).otherwise(1).as("archive_ind"))
      .distinct()
  }

  def createUsualPpContent: DataFrame = {

    dimSocParameter
      .join(productInfo.filter(!col("soc_code").isin(listConstructorSoc: _*)), Seq("soc_code", "market_key"))
      .select(
        col("soc_code"),
        lit(null).as("constructor_id"),
        col("market_key"),
        col("business_name_product_info"),
        col("product_id_product_info"),
        col("product_name_product_info"),
        col("product_category_product_info"),
        col("monthly_fee"),
        col("daily_fee"),
        when(col("cnt_voice_internal_local") >= col("cnt_voice_local"), col("cnt_voice_internal_local"))
          .otherwise(col("cnt_voice_local")).as("count_voice_min"),
        col("count_gprs").as("count_data_gb"),
        col("count_sms"),
        col("count_availabl_addl_ctn_family").as("count_available_family_ctn"),
        col("arhive_ind").as("archive_ind"))
      .distinct()
  }

  def calculateConstructorSoc: DataFrame = {
    val socV = serviceAgreement
    .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
    .filter(substring(col("soc_code"), 3, 1) === "V")

    val socD = serviceAgreement
      .filter(substring(col("soc_code"), 1, 3).isin(socPresetShortList: _*))
      .filter(substring(col("soc_code"), 3, 1) === "D")

    val res = socV.as("v").join(socD.as("d"),
      col("v.subs_key") === col("d.subs_key") && col("v.ban_key") === col("d.ban_key"), "inner")
      .select(
        col("v.subs_key"),
        col("v.ban_key"),
        greatest(col("v.sys_creation_date"), col("d.sys_creation_date")).as("sys_creation_date"),
        greatest(col("v.sys_update_date_corr"), col("d.sys_update_date_corr")).as("sys_update_date_corr"),
        least(col("v.expiration_date"), col("d.expiration_date")).as("expiration_date"),
        col("v.soc_code").as("voice_soc"),
        col("d.soc_code").as("data_soc"))
    res
  }
}
