package com.iobeam.spark.streams.testutils.triggers

import com.iobeam.spark.streams.TestTimeRecord
import com.iobeam.spark.streams.triggers.DeviceTimeoutTrigger
import org.apache.spark.streaming.{Milliseconds, Minutes, Seconds}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

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

            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2),100) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(0) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs - 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(Some(triggerEvent))
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(None)
            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(timeoutUs,2), 200) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs * 2) should equal(Some(triggerEvent))

    }

    "Threshold" should "trigger only once" in {

        val endUs = 1463661368 * 1000 * 1000
        val timeout = Minutes(160)
        val timeoutUs = timeout.milliseconds * 1000

        val trigger = new DeviceTimeoutTrigger(timeout, triggerEvent)

        var time: Long = 0
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 100) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 200) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += timeoutUs
        trigger.batchDoneUpdateAndTest(time) should equal(Some(triggerEvent))
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 200) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
    }

    "trigger" should "not trigger on reordered data" in {
        val timeout = Minutes(160)
        val timeoutUs = timeout.milliseconds * 1000

        var time: Long = timeoutUs
        val trigger = new DeviceTimeoutTrigger(timeout, triggerEvent)

        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 100) should equal(None)
        time += 1000

        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        // reordered data
        trigger.recordUpdateAndTest(new TestTimeRecord(-timeoutUs,2), 200) should equal(None)
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000

        trigger.recordUpdateAndTest(new TestTimeRecord(-timeoutUs + 100,2), 300) should equal(None)

        trigger.batchDoneUpdateAndTest(time) should equal(None)

    }
}
