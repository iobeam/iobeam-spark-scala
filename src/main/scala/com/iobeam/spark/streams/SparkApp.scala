package com.iobeam.spark.streams

import com.iobeam.spark.streams.config.{DeviceConfig, SeriesConfig}
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
    def processStream(stream: DStream[(String, (TimeRecord, DeviceConfig))]): OutputStreams

  /**
    * Returns the default configuration for the app.
    * Can be modified by the config REST Api.
    *
    * @return A set of device configs.
    */

  def getInitialConfigs: Seq[SeriesConfig] = Seq()

    //TODO: removing for now; discuss with Olaf
    //def usedTimeSeriesNames: Seq[DataSet.DataKey]

    override def toString: String = name
}
