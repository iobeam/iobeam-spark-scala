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
    val offlineEvent = "device offline"
    val onlineEvent = "device online"

    val timeoutUs = 10 * 1000 * 1000
    val timeout = Milliseconds(timeoutUs / 1000)
    val testTimeRecord = new TestTimeRecord(10,2)

    "Threshold" should "trigger when no data for timeout period" in {
            val deviceTimeoutTrigger = new DeviceTimeoutTrigger(timeout, offlineEvent)

            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2),100) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(0) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs - 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(Some(offlineEvent))
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(None)
            deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(timeoutUs,2), 200) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 1) should equal(None)
            deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs * 2) should equal(Some(offlineEvent))

    }

    "Threshold" should "trigger only once" in {

        val endUs = 1463661368 * 1000 * 1000
        val timeout = Minutes(160)
        val timeoutUs = timeout.milliseconds * 1000

        val trigger = new DeviceTimeoutTrigger(timeout, offlineEvent)

        var time: Long = 0
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 100) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 200) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += timeoutUs
        trigger.batchDoneUpdateAndTest(time) should equal(Some(offlineEvent))
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
        time += 1000
        trigger.recordUpdateAndTest(new TestTimeRecord(time,2), 200) should equal(None)
        time += 1000
        trigger.batchDoneUpdateAndTest(time) should equal(None)
    }

    "trigger" should "not trigger on reordered event times" in {
        val timeout = Minutes(160)
        val timeoutUs = timeout.milliseconds * 1000

        var time: Long = timeoutUs
        val trigger = new DeviceTimeoutTrigger(timeout, offlineEvent)

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

    "Threshold" should "trigger when no data for timeout period in processing time" in {
        val deviceTimeoutTrigger = new DeviceTimeoutTrigger(timeout, offlineEvent, onlineEvent, true)

        deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2),0) should equal(Some(onlineEvent))
        deviceTimeoutTrigger.batchDoneUpdateAndTest(1) should equal(None)
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs - 1) should equal(None)
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(Some(offlineEvent))
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 1) should equal(None)
        deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(timeoutUs,2), timeoutUs + 2) should equal(Some(onlineEvent))
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 3) should equal(None)
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs * 2) should equal(Some(offlineEvent))

    }

    "Threshold" should "not trigger on reordered data" in {
        val deviceTimeoutTrigger = new DeviceTimeoutTrigger(timeout, offlineEvent, onlineEvent, true)

        deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2),0) should equal(Some(onlineEvent))
        deviceTimeoutTrigger.batchDoneUpdateAndTest(1) should equal(None)
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs - 1) should equal(None)
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs) should equal(Some(offlineEvent))
        deviceTimeoutTrigger.batchDoneUpdateAndTest(timeoutUs + 1) should equal(None)

        deviceTimeoutTrigger.recordUpdateAndTest(new TestTimeRecord(0,2), timeoutUs + 2) should equal(None)
    }
}
