package ru.beeline.cvm.datamarts.transform.pilotsOutb

import org.apache.spark.sql.functions.{col, concat, lit}

object Main extends CustomParams {

   import org.apache.spark.sql.functions.concat_ws


   val res = spark.read.table(TableAggNbaPilot)
      .select(
         col("ctn"),
         concat_ws(";", col("nflag_pilot"), col("nflag_option")).as("nflag_outb")
      )

   val pilots2InfoOutb = spark.table(TableAggNbaPilot)
      .filter(col("nflag_pilot") === 2)
      .filter(col("nflag_option") < 90)
      .withColumn("add_info",
        concat(lit(s"↓t между звонками: интервалы снижены до "),
          col("nflag_option"),
          lit(" дней, не блокировать абонента")))
      .select("ctn", "add_info")

   res
      .repartition(1)
      .write
      .format("orc")
      .mode("overwrite")
      .insertInto(TableCvmFlags)

   pilots2InfoOutb
      .repartition(1)
      .write
      .format("orc")
      .mode("overwrite")
      .insertInto(TableCvmNbaOutboundAddInfo)

}
