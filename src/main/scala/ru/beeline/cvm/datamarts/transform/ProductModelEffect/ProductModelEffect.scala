package ru.beeline.cvm.datamarts.transform.ProductModelEffect

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

class ProductModelEffect extends Load {

  private val productModelEffectSource: DataFrame = getProductModelEffectSource(TableProductModelEffectSource)

  val res: DataFrame = productModelEffectSource
    .select(
      col("pred_effect"),
      col("product_id"),
      col("use_case_impact_id")
    )

}
