package ru.beeline.cvm.commons.logging

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class RecordLoggingTable(table_trg: String,
                              metric: String,
                              table_src: String,
                              partition_processed: String,
                              value_ts: Long)
object RecordLoggingTable {
  implicit val encoderGetMetric: Encoder[RecordLoggingTable] = deriveEncoder[RecordLoggingTable]
}
