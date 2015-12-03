package com.iobeam.spark

/**
  *
  * ==Overview==
  *
  * The iobeam spark library provides functionality to build a spark streaming app that can run
  * on iobeam. It provides both iobeam interface functionality as well as utility functions for
  * common IoT stream processing tasks.
  *
  * ===Setting up project with Maven===
  *
  * For quick setup of an iobeam spark project, you can use a Maven archetype.
  * {{{
  * mvn archetype:generate -DarchetypeArtifactId=spark-app-maven-archetype -DarchetypeGroupId=com.iobeam
  * }}}
  *
  * And follow the instructions.
  *
  * ==Usage==
  *
  * To implement an iobeam spark app, first inherit the class [[com.iobeam.spark.streams.SparkApp]].
  *
  * Example:
  *{{{
  *class StreamProcessor() extends SparkApp("MyAppName") {
  *
  *     override def processStream(stream: DStream[(String, (TimeRecord, DeviceConfig))]):
  *     OutputStreams = {
  *
  *         // Strip the configurations
  *         val outStream = stream.mapValues(a => a._1)
  *         new OutputStreams(new TimeSeriesStreamPartitioned("out_stream", outStream))
  *     }
  *}
  *
  *}}}
  *
  * The processStream function is where the stream processing code is written. The input stream is a
  * stream of spark RDDs of (String, (TimeRecord, DeviceConfig)) where the String is the deviceId that
  * uploaded the data, the [[com.iobeam.spark.streams.model.TimeRecord]] holds the data
  * uploaded, and the [[com.iobeam.spark.streams.config.DeviceConfig]] contains project global as
  * well as device specific configuration values.
  *
  * After analysis, processStream should return a [[com.iobeam.spark.streams.model.OutputStreams]]
  * with the generated DStreams of timeseries and trigger series.
  *
  * ==Model==
  *
  * The [[com.iobeam.spark.streams.model]] package contains the iobeam backend data abstractions.
  *
  * ==AppRunner==
  *
  * The [[com.iobeam.spark.streams.AppRunner]] is a utility to run spark streaming code with
  * local input.
  *
  * ==Util==
  *
  * The [[com.iobeam.spark.streams.util]] package contains utility functions.
  *
  *
  **/
package object streams

