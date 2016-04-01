package com.iobeam.spark.streams.filters

/**
  * Holds a filter and where its output should go.
  */
case class SeriesFilterConfiguration(outputSeries: String, filter: SeriesFilter)
