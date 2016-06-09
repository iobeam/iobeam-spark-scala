package com.iobeam.spark.streams.transforms

import com.iobeam.spark.streams.model.TimeRecord

/**
  * Triggers that need a full record or want more than one field
  */
abstract class NamespaceTransformState extends Serializable {

    /**
      * Called on each sample in series.
      *
      * @param record time record
      * @return Option[triggerString] id trigger
      */

    def recordUpdateAndTest(record: TimeRecord, batchTimeUs: Long): Option[Any]

    /**
      * Called when batch is done, used to update periodic state.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggeString]
      */
    def batchDoneUpdateAndTest(timeUs: Long): Option[Any]
}
