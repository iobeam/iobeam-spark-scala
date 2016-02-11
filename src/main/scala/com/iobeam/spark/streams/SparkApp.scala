package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.OutputStreams
/**
  * Defines the basic structure of a spark app.
  * Inherit from this class to implement an iobeam spark app.
  */

abstract class SparkApp(val name: String) extends Serializable {

    /**
      * Returns the OutputStreams representing the results of the spark computation.
      * Main data processing logic goes here.
      *
      * @return A set of output streams that iobeam's backend will handle,
      *         either as time series or trigger series.
      */
    def processStream(iobeamInterface: IobeamInterface): OutputStreams

    override def toString: String = name
}
