package com.iobeam.spark.streams.triggers

import com.iobeam.spark.streams.model.TimeRecord

/**
  * Triggers that need a full record or want all series
  */
trait DeviceTrigger extends Serializable {

    /**
      * Called on each sample in series.
      *
      * @param record time record
      * @return Option[triggerString] id trigger
      */

    def recordUpdateAndTest(record: TimeRecord, batchTimeUs: Long): Option[String]

    /**
      * Called when batch is done, used to update periodic state.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggeString]
      */
    def batchDoneUpdateAndTest(timeUs: Long): Option[String]

}
