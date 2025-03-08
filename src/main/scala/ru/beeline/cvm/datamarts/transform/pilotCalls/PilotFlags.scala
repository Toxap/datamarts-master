package ru.beeline.cvm.datamarts.transform.pilotCalls

import org.apache.spark.sql.functions.{udf, when, col}

import scala.util.Random

object PilotFlags extends CustomParams {

  val df = spark.range(1000)

  val generateRandomPhoneNumber: () => String = () => {
    val random = new Random()
    "9" + random.nextInt(100000000).toString.padTo(8, '0')
  }

  val generateRandomPhoneNumberUDF = udf(generateRandomPhoneNumber)

  val res = df
    .withColumn("ctn", generateRandomPhoneNumberUDF())
    .withColumn("nflag_outb", when(col("id")>=0 && col("id") <= 99, 1).otherwise(0))
    .drop("id")

  res
    .repartition(1)
    .write
    .format("orc")
    .mode("overwrite")
    .saveAsTable(TableCallsPilotFlag)


}
