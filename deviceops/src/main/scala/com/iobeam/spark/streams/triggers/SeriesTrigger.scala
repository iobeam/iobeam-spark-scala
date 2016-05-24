package com.iobeam.spark.streams.triggers

/**
  * Trigger attached to individual series
  */
trait SeriesTrigger extends Serializable {
    /**
      * Called on each sample in series.
      *
      * @param timeUs Sample time in us
      * @param batchTimeUs RDD time us
      * @param reading Value
      * @return Option[triggerString] id trigger
      */

    def sampleUpdateAndTest(timeUs: Long,
                            batchTimeUs: Long,
                            reading: Any): Option[String]
    /**
      * Called when batch is done, used to update periodic state regardless of series data.
      * May return trigger.
      *
      * @param timeUs Batch time
      * @return Option[triggerString]
      */
    def batchDoneUpdateAndTest(timeUs: Long): Option[String] = None
}
