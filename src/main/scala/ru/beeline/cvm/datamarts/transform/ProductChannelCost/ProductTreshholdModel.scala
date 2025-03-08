package ru.beeline.cvm.datamarts.transform.ProductChannelCost

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class ProductTreshholdModel extends Load {

        private val ProductChannelCost: DataFrame = getProductChannelCostSource(TableProductChannelCostSource)

        val res: DataFrame = ProductChannelCost
          .select(
                  col("cost_outbound"),
                  col("cost_1c"),
                  col("cost_email"),
                  col("cost_sms"),
                  col("cost_ussd"),
                  col("cost_robot"),
                  col("cost_lk"),
                  col("cost_dok"),
                  col("cost_mp"),
                  col("cost_stories"),
                  col("cost_push")
          )

}
