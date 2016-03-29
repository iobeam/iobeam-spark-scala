package com.iobeam.spark.streams.util

import com.iobeam.spark.streams.model.TimeRecord

/**
 * TimeRecord used for tests
 */
class TestTimeRecord(time: Long, val value: Int) extends TimeRecord(time) {
    override def toString: String = s"($time, $value)"

    override def equals(other: Any): Boolean =
        other match {
            case td: TestTimeRecord => time == td.time && value == td.value
            case _ => false
        }

    def getValue: Int = value
}
