package ru.beeline.cvm.datamarts.transform.upsell2plusDataset

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object Transformations {
  def aggregateRecharges(subscriberHistory: DataFrame,
                        loadDateMinusTwoMonth: String): DataFrame = subscriberHistory
    .filter(col("time_key") <= loadDateMinusTwoMonth)
    .na.fill(0, Seq("total_recharge_amt_rur", "cnt_recharge"))
    .groupBy(
      col("last_ban_key"),
      col("last_subs_key"),
      col("market_key_src").as("market_key")
    )
    .agg(
      sum("total_recharge_amt_rur").cast("double").as("balance_top_up_amount"),
      sum("cnt_recharge").cast("double").as("count_balance_top_up_amount")
    )

  def getSubscriberHistoryMinusTwoMonth(subscriberHistory: DataFrame,
                                        loadDateMinusTwoMonth: String,
                                        loadDateFirstDayOfMonth: String): DataFrame = subscriberHistory
    .filter(col("time_key") === loadDateMinusTwoMonth)
    .select(
      col("last_ban_key"),
      col("last_subs_key"),
      col("market_key_src").as("market_key"),
      col("time_key").as("time_key_src"),
      col("last_price_plan_key"),
      col("cnt_answerphone").cast("double"),
      col("contact_bln").cast("double"),
      col("total_contact").cast("double"),
      col("waiv_call_num").cast("double"),
      col("mou_in_bln").cast("double"),
      col("mou_in_mts").cast("double"),
      col("mou_in_mgf").cast("double"),
      col("mou_in_tl2").cast("double"),
      col("mou_out_bln").cast("double"),
      col("mou_out_mgf").cast("double"),
      col("out_voice_offnet_local_mgf").cast("double"),
      col("in_voice_offnet_local_mgf").cast("double"),
      col("in_voice_offnet_local_other").cast("double"),
      col("out_voice_fix_local").cast("double"),
      col("in_voice_fix_local").cast("double"),
      col("out_voice_offnet_mg").cast("double"),
      col("in_voice_offnet_mg").cast("double"),
      col("in_voice_fix_mg").cast("double"),
      col("voice_offnet_ics").cast("double"),
      col("voice_fix_ics").cast("double"),
      col("voice_out_h_local_rev").cast("double"),
      col("voice_out_h_mg_rev").cast("double"),
      col("data_h_rev").cast("double"),
      col("other_h_rev").cast("double"),
      col("other_int_rev").cast("double"),
      col("rc_rev").cast("double"),
      col("oc_rev").cast("double"),
      col("cnt_voice_out_day").cast("double"),
      col("cnt_voice_in_day").cast("double"),
      col("cnt_data_day").cast("double"),
      col("cnt_rev_day").cast("double"),
      col("out_voice_h_local").cast("double"),
      col("in_voice_h_local").cast("double"),
      col("out_voice_h_mg").cast("double"),
      col("in_voice_h_mg").cast("double"),
      col("in_voice_r_mg").cast("double"),
      col("data_mb").cast("double"),
      col("data_4g_mb").cast("double"),
      col("umcs_rev").cast("double"),
      col("ics_rev").cast("double"),
      col("other_revenue").cast("double"),
      col("service_margin").cast("double"),
      col("contribution_margin").cast("double"),
      col("days_in_status_c").cast("double"),
      col("contacts_group_all").cast("double"),
      coalesce(col("device_type"), lit("unknown")).as("device_type"),
      datediff(lit(loadDateFirstDayOfMonth), col("init_activation_date")).as("activation_days"),
      datediff(lit(loadDateFirstDayOfMonth), col("price_plan_change_date")).as("pp_days"),
      datediff(lit(loadDateFirstDayOfMonth), col("last_month_aab")).as("aab_days")
    )

  def getSubscriberHistoryWithRecharges(subscriberHistoryMinusTwoMonth: DataFrame,
                                        recharges: DataFrame): DataFrame = subscriberHistoryMinusTwoMonth
    .join(
      recharges,
      Seq("last_ban_key", "last_subs_key", "market_key"),
      "left"
    )

    def cleanCustomerAge(bans: DataFrame): DataFrame = bans
      .withColumn("customer_age",
          when(dayofyear(col("customer_birth_date")) - dayofyear(current_timestamp()) > 0,
              year(current_timestamp()) - year(col("customer_birth_date")) - 1)
            .otherwise(
                year(current_timestamp()) - year(col("customer_birth_date"))))
      .select(col("ban_key").as("last_ban_key"),
          regexp_replace(
              regexp_replace(coalesce(col("customer_gender"), lit("U")), "М", "M"),
              "W", "F").as("customer_gender"),
          when(col("customer_age") < 0, col("customer_age") * (-1))
            .otherwise(col("customer_age")).as("customer_age"),
          col("customer_birth_date")
      )

  def replaceNullInf(col1: Column, col2: Column): Column = coalesce(
      col1 / col2,
      when(col1 === 0 && col2 === 0, lit(222222))
        .when(col1 > 0, lit(999999))
        .otherwise(lit(-999999))
  )

    def getUpsellSample(subcribers: DataFrame,
                        subscriberHistoryWithRecharges: DataFrame,
                        bansCleanCustomerAge: DataFrame,
                        subscriberArpuAggregate: DataFrame,
                        tariffs: DataFrame,
                        partitionName: String): DataFrame =
      subcribers
      .join(subscriberHistoryWithRecharges, Seq("last_ban_key", "last_subs_key", "market_key"), "inner")
      .join(bansCleanCustomerAge, Seq("last_ban_key"), "left")
      .join(subscriberArpuAggregate, Seq("last_ban_key", "last_subs_key", "market_key"), "left").as("res")
      .join(tariffs.as("dim_soc"),
        col("res.last_price_plan_key") === col("dim_soc.soc_code") &&
          col("res.market_key") === col("dim_soc.market_key_dim_soc"),
        "left")
      .na.fill(0,
        Seq(
          "balance_top_up_amount",
          "count_balance_top_up_amount",
          "total_arpu",
          "out_voice_traffic",
          "in_voice_traffic",
          "out_sms_count",
          "data_traffic",
          "monthly_fee_dim_soc",
          "minutes",
          "gb",
          "sms"
        )
    )
      .select(
        col("last_ban_key").cast("bigint"),
        col("last_subs_key"),
        col("market_key"),
        col("customer_gender"),
        col("time_key_src"),
        col("last_price_plan_key"),
        col("monthly_fee_dim_soc").as("monthly_fee"),
        col("minutes"),
        col("gb"),
        col("sms"),
        col("cnt_answerphone"),
        col("contact_bln"),
        col("total_contact"),
        col("waiv_call_num"),
        col("mou_in_bln"),
        col("mou_in_mts"),
        col("mou_in_mgf"),
        col("mou_in_tl2"),
        col("mou_out_bln"),
        col("mou_out_mgf"),
        col("out_voice_offnet_local_mgf"),
        col("in_voice_offnet_local_mgf"),
        col("in_voice_offnet_local_other"),
        col("out_voice_fix_local"),
        col("in_voice_fix_local"),
        col("out_voice_offnet_mg"),
        col("in_voice_offnet_mg"),
        col("in_voice_fix_mg"),
        col("voice_offnet_ics"),
        col("voice_fix_ics"),
        col("voice_out_h_local_rev"),
        col("voice_out_h_mg_rev"),
        col("data_h_rev"),
        col("other_h_rev"),
        col("other_int_rev"),
        col("rc_rev"),
        col("oc_rev"),
        col("cnt_voice_out_day"),
        col("cnt_voice_in_day"),
        col("cnt_data_day"),
        col("cnt_rev_day"),
        col("out_voice_h_local"),
        col("in_voice_h_local"),
        col("out_voice_h_mg"),
        col("in_voice_h_mg"),
        col("in_voice_r_mg"),
        col("data_mb"),
        col("data_4g_mb"),
        col("umcs_rev"),
        col("ics_rev"),
        col("other_revenue"),
        col("service_margin"),
        col("contribution_margin"),
        col("days_in_status_c"),
        col("contacts_group_all"),
        col("device_type"),
        col("activation_days"),
        col("pp_days"),
        col("aab_days"),
        col("balance_top_up_amount"),
        col("count_balance_top_up_amount"),
        col("total_arpu"),
        col("out_voice_traffic"),
        col("in_voice_traffic"),
        col("out_sms_count"),
        col("data_traffic"),
        replaceNullInf(
            col("activation_days"),
            col("pp_days")
        ).as("ratio_act_pp_date"),
        replaceNullInf(
            col("total_arpu"),
            col("monthly_fee_dim_soc")
        ).as("ratio_arpu_curpp"),
        replaceNullInf(
            col("balance_top_up_amount"),
            col("total_arpu")
        ).as("ratio_bal_arpu"),
        replaceNullInf(
            col("balance_top_up_amount"),
            col("monthly_fee_dim_soc")
        ).as("ratio_bal_curpp"),
        replaceNullInf(
            col("out_voice_traffic"),
            col("minutes")
        ).as("ratio_voice_cur_minutes"),
        replaceNullInf(
            col("data_traffic"),
            col("gb")
        ).as("ratio_data_cur_gb"),
        replaceNullInf(
            col("out_sms_count"),
            col("sms")
        ).as("ratio_outsms_cur_sms"),
        lit(partitionName).as("time_key")
      )
      .distinct()
}
