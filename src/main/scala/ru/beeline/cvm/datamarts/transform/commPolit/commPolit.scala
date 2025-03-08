package ru.beeline.cvm.datamarts.transform.commPolit

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class commPolit extends Load {

  val base = getBase
  val dmMobileSubscriber = getDmMobileSubscriber
  val tCamps = getTCamps
  val cmsMembers = getCmsMembersClear
  val aggOutbound = getAggOutboundDetailHistory
  val tCampsFilters = getTCampsFilters

  val clust_2 = convertColumnTypes(clust, fctFeaturesWithTypes)
  val clust_3 = normalizeColumns(clust_2, colsToNormalize, daysInMonthCol)

  val feat_lag_agg_cols = createMeanAggColumns(featAggLagsCols, nullReplaces)
  val feat_lag_agg_cols2 = createStdAggColumns(featAggLagsCols, nullReplaces)
  val feat_lag_wout_agg_cols = createColumnsWithoutAggregation(featWoutAgg, nullReplaces)

  val sq = (feat_lag_agg_cols ++ feat_lag_agg_cols2 ++ feat_lag_wout_agg_cols).toSeq


  val last_subs_window = (
    Window
      .partitionBy(
        "subs_key",
        "ban_key",
        "market_key"
      )
      .orderBy(base("time_key_src").desc)
    );

  val base_clust = base.as("b")
    .join(
      clust_3.as("c"),
        base("subs_key") === clust_3("last_subs_key") &&
        base("ban_key") === clust_3("last_ban_key") &&
        base("market_key") === clust_3("market_key_src") &&
        floor(
          months_between(
            substring(base("time_key_src"), 1, 7),
            concat_ws(
              "-",
              substring(clust_3("clust_time_key"), 2, 4),
              substring(clust_3("clust_time_key"), 6, 2)
            )
          )
        ).between(1, 5),
      "left"
    )
    .groupBy(
      base("subs_key"),
      base("ban_key"),
      base("market_key")
    )
    .agg(sq.head, sq.tail :_*)
    .select(
      "subs_key",
      "ban_key",
      "market_key",
      "normalized_bs_data_rate_mean_lag2_4",
      "normalized_bs_data_rate_std_lag2_4",
      "normalized_bs_voice_rate_mean_lag2_4",
      "normalized_bs_voice_rate_std_lag2_4",
      "normalized_total_recharge_amt_rur_mean_lag2_4",
      "normalized_total_recharge_amt_rur_std_lag2_4",
      "normalized_cnt_recharge_mean_lag2_4",
      "normalized_cnt_recharge_std_lag2_4",
      "normalized_total_contact_mean_lag2_4",
      "normalized_total_contact_std_lag2_4",
      "normalized_cnt_voice_day_mean_lag2_4",
      "normalized_cnt_voice_day_std_lag2_4",
      "normalized_cnt_data_day_mean_lag2_4",
      "normalized_cnt_data_day_std_lag2_4",
      "normalized_direct_rev_mean_lag2_4",
      "normalized_direct_rev_std_lag2_4",
      "normalized_total_rev_mean_lag2_4",
      "normalized_total_rev_std_lag2_4",
      "normalized_data_mb_mean_lag2_4",
      "normalized_data_mb_std_lag2_4",
      "normalized_in_voice_mean_lag2_4",
      "normalized_in_voice_std_lag2_4",
      "normalized_out_voice_mean_lag2_4",
      "normalized_out_voice_std_lag2_4",
      "permanent_aab_lag2",
      "is_smartphone_lag2",
      "days_in_status_c_lag2",
      "pp_day_lag2");


  val cols = spark.read.table("biis.dim_ban_pub").columns.map(x => col(x).as(x.toLowerCase));
  val biis = spark.read.table("biis.dim_ban_pub").select(cols:_*);

  val dban = (
    biis
      .select(
        "ban_key",
        "market_key_src",
        "customer_birth_date",
        "customer_gender",
        "ban_activation_date_key"
      )
      .withColumnRenamed(
        "ban_key",
        "ban_key1"
      )
    );


  val age_col = (
    months_between(
      col("time_key_src"),
      col("customer_birth_date")
    )
      / lit(12)
    ).cast(IntegerType);

  val base_1 = (
    base
      .select(
        "subs_key",
        "ban_key",
        "market_key",
        "time_key_src"
      )
    );

  val base_dban =
    base_1.join(dban, base_1("ban_key") === dban("ban_key1") && base_1("market_key") === dban("market_key_src"), "left")
    .withColumn(
      "age",
      when(
        (age_col.isNull) || (age_col < 0), lit(-1)
      ).otherwise(age_col)
    )
      .na.fill(Map("customer_gender" -> "U"))
      .withColumn(
        "gender",
        when(col("customer_gender")==="M", lit("man"))
          .when(col("customer_gender")==="F", lit("woman"))
          .when(col("customer_gender")==="U", lit("unknown"))
          .when(col("customer_gender")==="W", lit("woman"))
          .when(col("customer_gender")==="М", lit("man"))
          .when(col("customer_gender").isNull, lit("unknown"))
      )
  .withColumn(
    "lifetime",
    datediff(
      col("time_key_src"),
      col("ban_activation_date_key")
    )
  )
    .drop("ban_key1", "market_key_src","customer_gender", "customer_birth_date", "ban_activation_date_key")
      .withColumn(
        "rn1",
        row_number().over(last_subs_window)
      )
      .filter(col("rn1") === 1)
      .drop("rn1")
    .select(
      "subs_key",
    "ban_key",
    "market_key",
    "age",
    "gender",
    "lifetime");

  val aggSms = dmMobileSubscriber.as("t")
    .join(cmsMembers.as("m"), Seq("subs_key", "ban_key", "market_key"))
    .join(tCamps.as("c"), Seq("camp_id"))
    .withColumn("utk", lit(SCORE_DATE))
    .filter(col("c.ussd_ind") === 0)
    .filter(col("c.technical_wevent_b2b_test") === 0)
    .filter(col("c.info_channel_name").like("%SMS%"))
    .filter(col("utk") < col("d90") && col("utk") > col("d0"))
    .groupBy("subs_key", "ban_key", "market_key", "utk")
    .agg(
      coalesce(count(when(col("utk") < col("d90"), 1)), lit(0)).as("sms_3m"),
      coalesce(count(when(col("utk") < col("d30"), 1)), lit(0)).as("sms_1m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Сохранение ") &&
            col("utk") < col("d90"), 1)), lit(0)).as("sms_sav_3m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Сохранение ") &&
            col("utk") < col("d30"), 1)), lit(0)).as("sms_sav_1m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Оптимизация/апгрейд тарифа") &&
            col("utk") < col("d90") && col("technical_wevent_b2b_test") === 0, 1)), lit(0)).as("sms_tar_3m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Оптимизация/апгрейд тарифа") &&
            col("utk") < col("d30") && col("technical_wevent_b2b_test") === 0, 1)), lit(0)).as("sms_tar_1m"),
      coalesce(count(
        when(
          col("product_b_new") === "TOP UP" &&
            col("utk") < col("d90"), 1)), lit(0)).as("sms_top_3m"),
      coalesce(count(
        when(
          col("product_b_new") === "TOP UP" &&
            col("utk") < col("d30"), 1)), lit(0)).as("sms_top_1m"),
      coalesce(count(
        when(
          col("product_b_new") === "Convergence" &&
            col("utk") < col("d90"), 1)), lit(0)).as("sms_con_3m"),
      coalesce(count(
        when(
          col("product_b_new") === "Convergence" &&
            col("utk") < col("d30"), 1)), lit(0)).as("sms_con_1m"),
      coalesce(count(
        when(
          col("product_b_new").isin(
            "Welcome",
            "Информ и прочее ",
            "Фин сервисы",
            "Девайсы ",
            "опции/ услуги ",
            "CPA",
            "TVE",
            "Roaming",
            "Семья") &&
            col("utk") < col("d90"), 1)), lit(0)).as("sms_inf_3m"),
      coalesce(count(
        when(
          col("product_b_new").isin(
            "Welcome",
            "Информ и прочее ",
            "Фин сервисы",
            "Девайсы ",
            "опции/ услуги ",
            "CPA",
            "TVE",
            "Roaming",
            "Семья") &&
            col("utk") < col("d30"), 1)), lit(0)).as("sms_inf_1m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
            col("utk") < col("d90"), 1)), lit(0)).as("sms_pr_3m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
            col("utk") < col("d30"), 1)), lit(0)).as("sms_pr_1m"),
      coalesce(sum(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
            col("utk") < col("d90"), col("sale_ind"))), lit(0)).as("sms_pr_3m_sale"),
      coalesce(sum(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
            col("utk") < col("d30"), col("sale_ind"))), lit(0)).as("sms_pr_1m_sale")
    )
    .select(col("t.subs_key"),
      col("t.ban_key"),
      col("t.market_key"),
      col("utk"),
      col("sms_3m"),
      col("sms_1m"),
      col("sms_sav_3m"),
      col("sms_sav_1m"),
      col("sms_tar_3m"),
      col("sms_tar_1m"),
      col("sms_top_3m"),
      col("sms_top_1m"),
      col("sms_con_3m"),
      col("sms_con_1m"),
      col("sms_inf_3m"),
      col("sms_inf_1m"),
      col("sms_pr_3m"),
      col("sms_pr_1m"),
      col("sms_pr_3m_sale"),
      col("sms_pr_1m_sale")
    );

  val members_agg_outbound = dmMobileSubscriber.as("t")
    .join(cmsMembers.as("m"), Seq("subs_key", "ban_key", "market_key"))
    .join(tCamps.as("c"), Seq("camp_id"))
    .withColumn("utk", lit(SCORE_DATE))
    .filter(col("c.ussd_ind") === 0)
    .filter(col("c.technical_wevent_b2b_test") === 0)
    .filter(col("c.info_channel_name").like("%Outbound%"))
    .filter(col("utk") < col("d365") && col("utk") >= col("d5"))
    .groupBy("subs_key", "ban_key", "market_key", "utk")
    .agg(
      coalesce(count(
        when(col("utk") < col("d365"), 1)), lit(0)).as("outb_12m"),
      coalesce(count(
        when(col("utk") < col("d180"), 1)), lit(0)).as("outb_6m"),
      coalesce(count(
        when(
          col("product_b_new") === "Сохранение " &&
          col("utk") < col("d365"), 1)), lit(0)).as("outb_sav_12m"),
      coalesce(count(
        when(
          col("product_b_new") === "Сохранение " &&
          col("utk") < col("d180"), 1)), lit(0)).as("outb_sav_6m"),
      coalesce(count(
        when(
          col("product_b_new") === "Оптимизация/апгрейд тарифа" &&
          col("utk") < col("d365") && col("technical_wevent_b2b_test") === 0, 1)), lit(0)).as("outb_tar_12m"),
      coalesce(count(
        when(
          col("product_b_new") === "Оптимизация/апгрейд тарифа" &&
          col("utk") < col("d180") && col("technical_wevent_b2b_test") === 0, 1)), lit(0)).as("outb_tar_6m"),
      coalesce(count(
        when(
          col("product_b_new") === "TOP UP" &&
          col("utk") < col("d365"), 1)), lit(0)).as("outb_top_12m"),
      coalesce(count(
        when(
          col("product_b_new") === "TOP UP" &&
          col("utk") < col("d180"), 1)), lit(0)).as("outb_top_6m"),
      coalesce(count(
        when(
          col("product_b_new") === "Convergence" &&
          col("utk") < col("d365"), 1)), lit(0)).as("outb_con_12m"),
      coalesce(count(
        when(
          col("product_b_new") === "Convergence" &&
          col("utk") < col("d180"), 1)), lit(0)).as("outb_con_6m"),
      coalesce(count(
        when(
          col("product_b_new").isin(
            "Welcome",
            "Информ и прочее ",
            "Фин сервисы",
            "Девайсы ",
            "опции/ услуги ",
            "CPA",
            "TVE",
            "Roaming",
            "Семья") &&
          col("utk") < col("d365"), 1)), lit(0)).as("outb_inf_12m"),
      coalesce(count(
        when(
          col("product_b_new").isin(
            "Welcome",
            "Информ и прочее ",
            "Фин сервисы",
            "Девайсы ",
            "опции/ услуги ",
            "CPA",
            "TVE",
            "Roaming",
            "Семья") &&
          col("utk") < col("d180"), 1)), lit(0)).as("outb_inf_6m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
          col("utk") < col("d365"), 1)), lit(0)).as("outb_pr_12m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
          col("utk") < col("d180"), 1)), lit(0)).as("outb_pr_6m"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
          col("utk") < col("d365"), col("sale_ind"))), lit(0)).as("outb_pr_12m_sale"),
      coalesce(count(
        when(
          col("product_b_new").isin("Convergence", "Оптимизация/апгрейд тарифа", "TOP UP", "Сохранение ") &&
          col("utk") < col("d180"), col("sale_ind"))), lit(0)).as("outb_pr_6m_sale")
    )
    .select(col("t.subs_key"),
      col("t.ban_key"),
      col("t.market_key"),
      col("utk"),
      col("outb_12m"),
      col("outb_6m"),
      col("outb_sav_12m"),
      col("outb_sav_6m"),
      col("outb_tar_12m"),
      col("outb_tar_6m"),
      col("outb_top_12m"),
      col("outb_top_6m"),
      col("outb_con_12m"),
      col("outb_con_6m"),
      col("outb_inf_12m"),
      col("outb_inf_6m"),
      col("outb_pr_12m"),
      col("outb_pr_6m"),
      col("outb_pr_12m_sale"),
      col("outb_pr_6m_sale")
    );

  val res = dmMobileSubscriber
    .join(aggSms.as("a"), Seq("subs_key", "ban_key", "market_key"), "left")
    .join(members_agg_outbound.as("m"), Seq("subs_key", "ban_key", "market_key"), "left")
    .select(
      col("subs_key"),
      col("ban_key"),
      col("market_key"),
      coalesce(col("sms_3m"), lit(0)).as("sms_3m"),
      coalesce(col("sms_1m"), lit(0)).as("sms_1m"),
      coalesce(col("sms_sav_3m"), lit(0)).as("sms_sav_3m"),
      coalesce(col("sms_sav_1m"), lit(0)).as("sms_sav_1m"),
      coalesce(col("sms_tar_3m"), lit(0)).as("sms_tar_3m"),
      coalesce(col("sms_tar_1m"), lit(0)).as("sms_tar_1m"),
      coalesce(col("sms_top_3m"), lit(0)).as("sms_top_3m"),
      coalesce(col("sms_top_1m"), lit(0)).as("sms_top_1m"),
      coalesce(col("sms_con_3m"), lit(0)).as("sms_con_3m"),
      coalesce(col("sms_con_1m"), lit(0)).as("sms_con_1m"),
      coalesce(col("sms_inf_3m"), lit(0)).as("sms_inf_3m"),
      coalesce(col("sms_inf_1m"), lit(0)).as("sms_inf_1m"),
      coalesce(col("sms_pr_3m"), lit(0)).as("sms_pr_3m"),
      coalesce(col("sms_pr_1m"), lit(0)).as("sms_pr_1m"),
      coalesce(col("sms_pr_3m_sale"), lit(0)).as("sms_pr_3m_sale"),
      coalesce(col("sms_pr_1m_sale"), lit(0)).as("sms_pr_1m_sale"),
      coalesce(col("outb_12m"), lit(0)).as("outb_12m"),
      coalesce(col("outb_6m"), lit(0)).as("outb_6m"),
      coalesce(col("outb_sav_12m"), lit(0)).as("outb_sav_12m"),
      coalesce(col("outb_sav_6m"), lit(0)).as("outb_sav_6m"),
      coalesce(col("outb_tar_12m"), lit(0)).as("outb_tar_12m"),
      coalesce(col("outb_tar_6m"), lit(0)).as("outb_tar_6m"),
      coalesce(col("outb_top_12m"), lit(0)).as("outb_top_12m"),
      coalesce(col("outb_top_6m"), lit(0)).as("outb_top_6m"),
      coalesce(col("outb_con_12m"), lit(0)).as("outb_con_12m"),
      coalesce(col("outb_con_6m"), lit(0)).as("outb_con_6m"),
      coalesce(col("outb_inf_12m"), lit(0)).as("outb_inf_12m"),
      coalesce(col("outb_inf_6m"), lit(0)).as("outb_inf_6m"),
      coalesce(col("outb_pr_12m"), lit(0)).as("outb_pr_12m"),
      coalesce(col("outb_pr_6m"), lit(0)).as("outb_pr_6m"),
      coalesce(col("outb_pr_12m_sale"), lit(0)).as("outb_pr_12m_sale"),
      coalesce(col("outb_pr_6m_sale"), lit(0)).as("outb_pr_6m_sale")
    );

  val res2 = dmMobileSubscriber
    .join(aggOutbound, Seq("subs_key", "ban_key", "market_key"), "left")
    .join(tCampsFilters, Seq("camp_id"))
    .groupBy("subs_key", "ban_key", "market_key", "utk")
    .agg(
      max(
        coalesce(
          when(col("camp_id").isNotNull,
            least(datediff(col("utk"), to_date(aggOutbound("response_date_src"))),
              lit(365))), lit(365)))
        .as("last_try_call_days"),
      max(
        coalesce(
          when(aggOutbound("contact_ind") === 1 && col("camp_id").isNotNull,
            least(datediff(col("utk"), to_date(aggOutbound("response_date_src"))),
              lit(365))), lit(365)))
        .as("last_contact_call_days"),
      max(
        coalesce(
          when(aggOutbound("sy_ind") === 1 && col("camp_id").isNotNull,
            least(datediff(col("utk"), to_date(aggOutbound("response_date_src"))),
              lit(365))), lit(365)))
        .as("last_sy_call_days"))
    .select(
      dmMobileSubscriber("subs_key"),
      dmMobileSubscriber("ban_key"),
      dmMobileSubscriber("market_key"),
      col("last_try_call_days"),
      col("last_contact_call_days"),
      col("last_sy_call_days"));

  val resCommPolit = dmMobileSubscriber
    .join(base_dban, Seq("subs_key", "ban_key", "market_key"), "left")
    .join(base_clust, Seq("subs_key", "ban_key", "market_key"), "left")
    .join(res, Seq("subs_key", "ban_key", "market_key"), "left")
    .join(res2, Seq("subs_key", "ban_key", "market_key"), "left")
    .drop("utk");


  val FinalResult = resCommPolit
    .withColumn("report_dt", lit(SCORE_DATE))
    .select(col("subs_key"),
      col("market_key"),
      col("ban_key").cast("long"),
      col("sms_3m"),
      col("sms_1m").cast("long"),
      col("sms_sav_3m"),
      col("sms_sav_1m"),
      col("sms_tar_3m"),
      col("sms_tar_1m"),
      col("sms_top_3m"),
      col("sms_top_1m"),
      col("sms_con_3m"),
      col("sms_con_1m"),
      col("sms_inf_3m"),
      col("sms_inf_1m"),
      col("sms_pr_3m"),
      col("sms_pr_1m"),
      col("sms_pr_3m_sale"),
      col("sms_pr_1m_sale"),
      col("outb_12m"),
      col("outb_6m"),
      col("outb_sav_12m"),
      col("outb_sav_6m"),
      col("outb_tar_12m"),
      col("outb_tar_6m"),
      col("outb_top_12m"),
      col("outb_top_6m"),
      col("outb_con_12m"),
      col("outb_con_6m"),
      col("outb_inf_12m"),
      col("outb_inf_6m"),
      col("outb_pr_12m"),
      col("outb_pr_6m"),
      col("outb_pr_12m_sale"),
      col("outb_pr_6m_sale"),
      col("last_try_call_days"),
      col("last_contact_call_days"),
      col("last_sy_call_days"),
      col("normalized_bs_data_rate_mean_lag2_4"),
      col("normalized_bs_data_rate_std_lag2_4"),
      col("normalized_bs_voice_rate_mean_lag2_4"),
      col("normalized_bs_voice_rate_std_lag2_4"),
      col("normalized_total_recharge_amt_rur_mean_lag2_4"),
      col("normalized_total_recharge_amt_rur_std_lag2_4"),
      col("normalized_cnt_recharge_mean_lag2_4"),
      col("normalized_cnt_recharge_std_lag2_4"),
      col("normalized_total_contact_mean_lag2_4"),
      col("normalized_total_contact_std_lag2_4"),
      col("normalized_cnt_voice_day_mean_lag2_4"),
      col("normalized_cnt_voice_day_mean_lag2_4"),
      col("normalized_cnt_data_day_mean_lag2_4"),
      col("normalized_cnt_data_day_std_lag2_4"),
      col("normalized_direct_rev_mean_lag2_4"),
      col("normalized_direct_rev_std_lag2_4"),
      col("normalized_total_rev_mean_lag2_4"),
      col("normalized_total_rev_std_lag2_4"),
      col("normalized_data_mb_mean_lag2_4"),
      col("normalized_data_mb_std_lag2_4"),
      col("normalized_in_voice_mean_lag2_4"),
      col("normalized_in_voice_std_lag2_4"),
      col("normalized_out_voice_mean_lag2_4"),
      col("normalized_out_voice_std_lag2_4"),
      col("permanent_aab_lag2"),
      col("is_smartphone_lag2"),
      col("days_in_status_c_lag2"),
      col("pp_day_lag2"),
      col("age"),
      col("gender"),
      col("lifetime"),
      col("report_dt")
    )

  }
