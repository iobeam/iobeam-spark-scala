package com.iobeam.spark.streams.filters

/**
  * Filters
  */

abstract class SeriesFilter extends Serializable {

    def update(t: Long, batchTimeUs:Long, newSample: Any): Any

    def getValue: Any
}
