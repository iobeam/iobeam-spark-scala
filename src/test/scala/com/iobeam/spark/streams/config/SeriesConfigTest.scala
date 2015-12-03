package com.iobeam.spark.streams.config

import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests for TimeSeriesConfig
 */
class SeriesConfigTest extends FlatSpec with Matchers {

    "A SeriesConfig" should "have proper values set" in {
        val conf = new SeriesConfig("testDevice", "temp", "foo", 2)
        conf.timeSeriesName should equal("temp")
        conf.get("foo").get should equal(2)
    }
}
