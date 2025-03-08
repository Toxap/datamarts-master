package ru.beeline.cvm.commons.logging

import io.circe.{Decoder, HCursor, Json}
import cats.syntax.either._
import io.circe.parser.{decode, parse}

case class InfoCsvFile(path: String,
                       tableTrgName: String,
                       tableSrcName: String,
                       nameMetric: String)

object InfoCsvFile{
  implicit val infoCsvFileDecoder: Decoder[InfoCsvFile] = new Decoder[InfoCsvFile]{

    final def apply(cursor: HCursor): Decoder.Result[InfoCsvFile] =
      for {
        path <- cursor.get[String]("path")
          tableTrgName <- cursor.get[String]("tableTrgName")
          tableSrcName <- cursor.get[String]("tableSrcName")
          nameMetric <- cursor.get[String]("nameMetric")
      } yield {
        InfoCsvFile(path = path, tableTrgName = tableTrgName,
          tableSrcName = if (tableSrcName != "null") tableSrcName else null, nameMetric = nameMetric)
      }
  }
}
