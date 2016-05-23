package com.iobeam.spark.streams.triggers

import org.apache.spark.streaming.Duration

private object TriggerNames {
    val TRIGGER = "TRIGGER"
    val RELEASE = "RELEASE"
}

/**
  * Trigger that sends an event when a value is above / below a threshold longer than
  * timeout. The threshold values is configured the same way as in [[ThresholdTrigger]].
  * Will only fire a trigger on samples, not on batch complete.
  */
class ThresholdTimeoutTrigger private(triggerLevel: Double, releaseLevel: Double, timeout: Duration,
                                      event: String, releaseEvent: Option[String])
    extends ThresholdTrigger(triggerLevel,
        TriggerNames.TRIGGER,
        releaseLevel,
        TriggerNames.RELEASE) {

    var thresholdTriggerState = false
    var timeoutTriggerState = false
    var stateChangeTime: Long = 0

    /**
      * Trigger that only sends event when it triggers.
      */

    def this(triggerLevel: Double, releaseLevel: Double, timeout: Duration, event: String) = {
        this(triggerLevel, releaseLevel, timeout, event, None)
    }

    /**
      * Trigger that sends events on trigger and release
      */
    def this(triggerLevel: Double, releaseLevel: Double, timeout: Duration,
             event: String, releaseEvent: String) = {
        this(triggerLevel, releaseLevel, timeout, event, Some(releaseEvent))
    }


    /**
      * Called on each sample in series.
      *
      * @param timeUs      Sample time in us
      * @param batchTimeUs RDD time us
      * @param reading     Value
      * @return Option[triggerString] id trigger
      */
    override def sampleUpdateAndTest(timeUs: Long, batchTimeUs: Long,
                                     reading: Any): Option[String] = {

        val thresholdEvent = super.sampleUpdateAndTest(timeUs, batchTimeUs, reading)

        if (thresholdEvent.isDefined) {

            thresholdEvent.get match {
                case TriggerNames.TRIGGER =>
                    if (!thresholdTriggerState) {
                        // Start to count time the threshold trigger is fired
                        stateChangeTime = timeUs
                        thresholdTriggerState = true
                    }
                case TriggerNames.RELEASE =>
                    thresholdTriggerState = false

                    if (timeoutTriggerState) {
                        timeoutTriggerState = false
                        return releaseEvent
                    }

            }
        }

        // If this is not triggered and the threshold trigger has triggered longer than timeout ago
        if (!timeoutTriggerState && thresholdTriggerState
            && (timeUs - stateChangeTime) > (timeout.milliseconds * 1000.0)) {
            // timeout trigger has fired
            timeoutTriggerState = true
            Some(event)
        } else {
            None
        }

    }

}
