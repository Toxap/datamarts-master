package ru.beeline.cvm.commons.logging

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class RecordLoggingTableLz(table_trg: String,
                                metric: String,
                                table_src: String,
                                partition_processed: String,
                                value_ts: Long,
                                load_lz: Long
                               )

object RecordLoggingTableLz {
  implicit val encoderGetMetric: Encoder[RecordLoggingTableLz] = deriveEncoder[RecordLoggingTableLz]
}
