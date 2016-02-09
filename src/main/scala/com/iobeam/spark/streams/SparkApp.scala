package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.{OutputStreams, TimeRecord}
import org.apache.spark.streaming.dstream.DStream

/**
  * Defines the basic structure of a spark app.
  * Inherit from this class to implement an iobeam spark app.
  */

abstract class SparkApp(val name: String) extends Serializable {

    /**
      * Returns the OutputStreams representing the results of the spark computation.
      * Main data processing logic goes here.
      *
      * @param stream The stream of device data from iobeam's backend.
      * @return A set of output streams that iobeam's backend will handle,
      *         either as time series or trigger series.
      */
    def processStream(stream: DStream[(String, TimeRecord)]): OutputStreams

    override def toString: String = name
}
