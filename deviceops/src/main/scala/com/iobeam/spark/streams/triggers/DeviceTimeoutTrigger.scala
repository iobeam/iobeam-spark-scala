package com.iobeam.spark.streams.triggers

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.Duration

/**
  * Trigger that detects when a device is offline
  */
class DeviceTimeoutTrigger(val timeout: Duration, event: String) extends DeviceTrigger {

    var lastReceivedEventTime: Long = 0
    // Start as triggered in case first data sent is old
    var isTriggered = true
    val timeoutUs = timeout.milliseconds * 1000
    /**
      * Called on each sample in series.
      *
      * @param record time record
      * @return Option[triggerString] id trigger
      */
    override def recordUpdateAndTest(record: TimeRecord, batchTimeUs: Long): Option[String] = {

        // Ignore reordered
        if (lastReceivedEventTime < record.time) {
            lastReceivedEventTime = record.time
        }

        // Does the new sample count as the device is online
        if ((batchTimeUs - lastReceivedEventTime) < timeoutUs) {
            isTriggered = false
        }

        None
    }

    /**
      * Called when batch is done, used to update periodic state.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggerString]
      */
    override def batchDoneUpdateAndTest(timeUs: Long): Option[String] = {

        if(isTriggered) return None

        val timeSinceSeenUs = timeUs - lastReceivedEventTime
        if (timeSinceSeenUs >= timeoutUs) {
           isTriggered = true
           return Some(event)
        }

        None
    }
}
