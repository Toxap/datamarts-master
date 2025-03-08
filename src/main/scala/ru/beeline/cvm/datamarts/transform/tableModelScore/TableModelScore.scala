package ru.beeline.cvm.datamarts.transform.tableModelScore

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class TableModelScore extends Load {

  val prduct1 = product1;

  val prduct2 = product2;

  val prduct3 = product3;

  val prduct4 = product4;

  val prduct6 = product6;

  val prduct8 = product8;

  val prduct9 = product9;

  val prduct10 = product10;

  val prduct11 = product11;

  val prduct12 = product12;

  val prduct13 = product13;

  val prduct14 = product14;

  val prduct15 = product15;

  val prduct16 = product16;

  val prduct18 = product18;

  val prduct23 = product23;

  val tableModelScore = prduct1
    .unionByName(prduct2)
    .unionByName(prduct3)
    .unionByName(prduct4)
    .unionByName(prduct6)
    .unionByName(prduct8)
    .unionByName(prduct9)
    .unionByName(prduct10)
    .unionByName(prduct11)
    .unionByName(prduct12)
    .unionByName(prduct13)
    .unionByName(prduct14)
    .unionByName(prduct15)
    .unionByName(prduct16)
    .unionByName(prduct18)
    .unionByName(prduct23);




}
