package com.iobeam.spark.streams.config

import org.scalatest.{FlatSpec, Matchers}

/**
 * Tests ConfigStore logic.
 */
class ConfigStoreTest extends FlatSpec with Matchers {
    val DEVICE_CONFIG = new DeviceConfig("testDevice",
        Map("testSeries" -> new SeriesConfig("testDevice", "testSeries", Map("foo" -> 1.0))))

    "Store" should "return device-specific default config for a time series" in {
        val store = new ConfigStore().update(new DeviceConfig(DEVICE_CONFIG.deviceId,
            Map("*" -> new SeriesConfig(DEVICE_CONFIG.deviceId, "*", Map("foo" -> 2.0)),
               "series" -> new SeriesConfig(DEVICE_CONFIG.deviceId, "series", Map("foo" -> 1.0))
            )))

        store.get("non-existant").get("non-existant", "foo") should equal(None)
        store.get(DEVICE_CONFIG.deviceId).get("non-existant", "foo") should equal(Some(2.0))
        store.get(DEVICE_CONFIG.deviceId).get("series", "foo") should equal(Some(1.0))

        val store2 = store.update(new DeviceConfig("*",
            Map("*" -> new SeriesConfig(DEVICE_CONFIG.deviceId, "*", Map("foo" -> 3.0)))))
        store2.get("non-existant").get("non-existant", "foo") should equal(Some(3.0))
    }
}
