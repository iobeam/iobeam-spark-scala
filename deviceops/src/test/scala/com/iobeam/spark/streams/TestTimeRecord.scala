package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord

/**
  * Used for tests
  */
class TestTimeRecord(override val time: Long,
                     val device_id: String,
                     val value: Double)
    extends TimeRecord(time,
        Map("value" -> value, "device_id" -> device_id, "namespace" -> "input")) {

    def this(time: Long, value: Double) = {
        this(time, "test_device", value)
    }

    def getValue: Double = value
}