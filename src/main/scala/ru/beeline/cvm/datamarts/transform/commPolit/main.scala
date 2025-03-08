package ru.beeline.cvm.datamarts.transform.commPolit

object main extends commPolit {

  FinalResult
    .repartition(5)
    .write
    .mode("overwrite")
    .format("orc")
    .insertInto(TableCommPoliteSample)

}
