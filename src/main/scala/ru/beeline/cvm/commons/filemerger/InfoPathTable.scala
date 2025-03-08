package ru.beeline.cvm.commons.filemerger

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class InfoPathTable(path: String,
                         exclusion: Int,
                         repartition: Int
                        )

object InfoPathTable{
  implicit val infoPathTableDecoder: Decoder[InfoPathTable] = deriveDecoder[InfoPathTable]
}
