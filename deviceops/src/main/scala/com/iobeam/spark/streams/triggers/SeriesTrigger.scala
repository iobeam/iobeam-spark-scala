package com.iobeam.spark.streams.triggers

import com.iobeam.spark.streams.model.{TimeRecord, TriggerEvent}

import scala.collection.mutable.ListBuffer

/**
  * Trigger attached to individual series
  */
trait SeriesTrigger extends Serializable {
    /**
      * Called on each sample in series.
      *
      * @param timeUs Sample time in us
      * @param reading Value
      * @return Option[triggerString] id trigger
      */

    def sampleUpdateAndTest(timeUs: Long, reading: Any): Option[String]

    /**
      * Called when batch is done, used to update periodic state regardless of series data.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggeString]
      */
    def batchDoneUpdateAndTest(timeUs: Long): Option[String] = None
}
