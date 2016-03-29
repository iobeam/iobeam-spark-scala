package com.iobeam.spark.streams.triggers

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.Duration

/**
  * Trigger that detects when a device is offline
  */
class DeviceTimeoutTrigger(val timeout: Duration, event: String) extends DeviceTrigger {

    var lastReceived: Long = 0
    var isTriggered = false

    /**
      * Called on each sample in series.
      *
      * @param record time record
      * @return Option[triggerString] id trigger
      */
    override def recordUpdateAndTest(record: TimeRecord): Option[String] = {
        lastReceived = record.time
        isTriggered = false
        None
    }

    /**
      * Called when batch is done, used to update periodic state.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggeString]
      */
    override def batchDoneUpdateAndTest(timeUs: Long): Option[String] = {

        if(isTriggered) return None

        if(timeUs - lastReceived >= timeout.milliseconds * 1000) {
           isTriggered = true
           return Some(event)
        }

        None
    }
}
