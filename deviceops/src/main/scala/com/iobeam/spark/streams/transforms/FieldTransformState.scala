package com.iobeam.spark.streams.transforms

/**
  * Transform attached to individual field in a namespace
  */
abstract class FieldTransformState extends Serializable {
    /**
      * Called on each sample in series.
      *
      * @param timeUs      Sample time in us
      * @param batchTimeUs RDD time us
      * @param reading     Value
      * @return Option[Any] transform output
      */

    def sampleUpdateAndTest(timeUs: Long,
                            batchTimeUs: Long,
                            reading: Any): Option[Any]

    /**
      * Called when batch is done, used to update periodic state regardless of series data.
      *
      * @param timeUs Batch time
      * @return Option[output]
      */
    def batchDoneUpdateAndTest(timeUs: Long): Option[Any] = None
}

