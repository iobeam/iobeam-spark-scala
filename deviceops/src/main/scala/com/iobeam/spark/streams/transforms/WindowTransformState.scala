package com.iobeam.spark.streams.transforms

import org.apache.spark.streaming.Duration

/**
  * Apply a trigger function on a time window of data
  */
class WindowTransformState(windowLen: Duration, func: List[(Long, Any)] => Boolean,
                           triggerEventName: String) extends FieldTransformState {

    val windowLenUs = windowLen.milliseconds * 1000
    var window = new scala.collection.mutable.ListBuffer[(Long, Double)]()
    var triggerState = false

    def create: FieldTransformState = new WindowTransformState(windowLen, func, triggerEventName)

    /**
      * Called on each sample in series.
      *
      * @param timeUs      Sample time in us
      * @param batchTimeUs RDD time in us
      * @param reading     Value
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

class WindowTransform(windowLen: Duration, func: List[(Long, Any)] => Boolean,
                      triggerEventName: String) extends FieldTransform {
    override def getNewTransform: FieldTransformState = new WindowTransformState(windowLen, func,
        triggerEventName)
}