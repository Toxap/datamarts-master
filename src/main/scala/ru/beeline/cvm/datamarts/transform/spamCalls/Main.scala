package ru.beeline.cvm.datamarts.transform.spamCalls

object Main extends spamCalls {

  incomeSpamCallsFin
    .repartition(NUM_PARTITIONS)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableSpamCalls)

}
