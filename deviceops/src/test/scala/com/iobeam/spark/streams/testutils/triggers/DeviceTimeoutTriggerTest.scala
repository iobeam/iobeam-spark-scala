package com.iobeam.spark.streams.testutils.triggers

import com.iobeam.spark.streams.TestTimeRecord
import com.iobeam.spark.streams.triggers.DeviceTimeoutTrigger
import org.apache.spark.streaming.{Milliseconds, Seconds}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, FlatSpec}

/**
  * Test device offline
  */
class DeviceTimeoutTriggerTest extends FlatSpec with Matchers with MockitoSugar {
    val triggerEvent = "device offline"

    val timeoutUs = 10 * 1000 * 1000
    val timeout = Milliseconds(timeoutUs / 1000)
    val testTimeRecord = new TestTimeRecord(10,2)

    "Threshold" should "trigger when no data for timeout period" in {
            val deviceTimeoutTrigger = new DeviceTimeoutTrigger(timeout, triggerEvent)

            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2)) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(0) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs - 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(Some(triggerEvent))
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(None)
            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(timeoutUs,2)) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs * 2) should equal(Some(triggerEvent))

    }

}
