package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord

/**
  * Used for tests
  */
class TestTimeRecord(override val time: Long,
                     val value: Double) extends TimeRecord(time, Map("value" -> value)) {
    def getValue: Double = value
}