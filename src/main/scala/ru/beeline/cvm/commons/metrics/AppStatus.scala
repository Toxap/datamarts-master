package ru.beeline.cvm.commons.metrics

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class AppStatus(product: String,
                     process: String,
                     source: String,
                     target: String,
                     value: Double)

object AppStatus{
  implicit val appStatusDecoder: Decoder[AppStatus] = deriveDecoder[AppStatus]
}
