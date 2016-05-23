package com.iobeam.spark.streams.triggers

import org.apache.spark.streaming.Duration

/**
  * Apply a trigger function on a time window of data
  */
class WindowTrigger(windowLen: Duration, func: List[(Long, Any)] => Boolean,
                            triggerEventName: String) extends SeriesTrigger {

    val windowLenUs = windowLen.milliseconds * 1000
    var window = new scala.collection.mutable.ListBuffer[(Long, Double)]()
    var triggerState = false

    /**
      * Called on each sample in series.
      *
      * @param timeUs  Sample time in us
      * @param batchTimeUs RDD time in us
      * @param reading Value
      * @return Option[triggerString] id trigger
      */
    override def sampleUpdateAndTest(timeUs: Long,
                                     batchTimeUs: Long,
                                     reading: Any): Option[String] = {

        if (!reading.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Window trigger only accepts Double series")
        }
        // add new sample
        window += ((timeUs, reading.asInstanceOf[Double]))

        // Remove samples that are to old
        window = window.filter(a => a._1 > batchTimeUs - windowLenUs)

        if (window.isEmpty) {
            return None
        }

        if (func(window.toList)) {
            if (triggerState) {
                // already triggered
                return None
            }
            triggerState = true
            return Some(triggerEventName)
        }

        triggerState = false
        None
    }
}