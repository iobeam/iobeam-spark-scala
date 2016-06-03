package com.iobeam.spark.streams.triggers

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.Duration

/**
  * Trigger that detects when a device is offline
  */
class DeviceTimeoutTrigger(val timeout: Duration,
                           val offlineEvent: String,
                           val onlineEvent: Option[String],
                           val useEventTime: Boolean)
    extends DeviceTrigger {

    /*
       Default behavior is to use event time and have no Online event
     */

    def this(timeout: Duration, event: String) {
        this(timeout, event, None, true)
    }

    def this(timeout: Duration, event: String, onlineEvent: String) {
        this(timeout, event, Some(onlineEvent), true)
    }

    def this(timeout: Duration, event: String, onlineEvent: String, useEventTime: Boolean) {
        this(timeout, event, Some(onlineEvent), useEventTime)
    }

    def create : DeviceTrigger = new DeviceTimeoutTrigger(timeout, offlineEvent, onlineEvent, useEventTime)

    var lastReceivedTime: Long = 0
    // Start as triggered in case first data sent is old
    var isOffline = true
    val timeoutUs = timeout.milliseconds * 1000

    /**
      * Called on each sample in series.
      *
      * @param record time record
      * @return Option[triggerString] id trigger
      */
    override def recordUpdateAndTest(record: TimeRecord, batchTimeUs: Long): Option[String] = {

        val newReceivedTime = if (useEventTime) {
            record.time
        } else {
            batchTimeUs
        }

        // Ignore reordered
        if (lastReceivedTime < newReceivedTime) {
            lastReceivedTime = newReceivedTime
        }

        // Does the new sample count as the device is online
        // If processing time is used this will never be true
        // as (batchTimeUs - lastReceivedTime) will always be 0
        if ((batchTimeUs - lastReceivedTime) < timeoutUs) {
            // Device is online
            // If it was offline, return onlineEvent
            if (isOffline) {
                isOffline = false
                return  onlineEvent
            }
            // Already online
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

        if(isOffline) return None

        val timeSinceSeenUs = timeUs - lastReceivedTime
        if (timeSinceSeenUs >= timeoutUs) {
           isOffline = true
           return Some(offlineEvent)
        }

        None
    }
}
