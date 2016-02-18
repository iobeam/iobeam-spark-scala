package com.iobeam.spark.streams.model

import org.apache.spark.streaming.dstream.DStream

/**
 * The base class representing all the outputs of a spark app.
 */
sealed abstract class Stream

/**
  * A collection of output streams representing all the results of a spark app.
  */
class OutputStreams private (val stream: Seq[Stream]) {

  def this(timeSeries: TimeSeriesStream) {
    this(Seq(timeSeries))
  }

  def this(triggerSerie: TriggerStream) {
    this(Seq(triggerSerie))
  }

  def this(timeSeries: TimeSeriesStream, triggerSeries: TriggerStream) {
    this(Seq(timeSeries, triggerSeries))
  }

  // Allow both orders of args to not break when we go back to varargs
  def this(triggerSeries: TriggerStream, timeSeries: TimeSeriesStream) {
    this(Seq(timeSeries, triggerSeries))
  }

  /**
    * Returns all of the [[TimeSeriesStream]]s
    * @return
      */
  def getTimeSeries: Seq[TimeSeriesStream] = stream.flatMap{
    case t: TimeSeriesStream => List(t)
    case _=> List()
  }

  //TODO: should this be a Seq. When do we need more than one TriggerStream
  /**
    * Return all of the [[TriggerStream]]s
    * @return
      */
  def getTriggerSeries: Seq[TriggerStream] = stream.flatMap{
    case t: TriggerStream => List(t)
    case _=> List()
  }
}

/**
  * Base class for all TimeSeriesStreams. These are the derived data
  * streams.
  */
abstract class TimeSeriesStream extends Stream {
  def getDStream: DStream[(String, _ <: TimeRecord)]
}

/**
  * A TimeSeriesStream with an explicit partition key, which should be a deviceID.
  * The partition key determines how the output is partioned for writing.
  * Thus, for efficiency, it should not be correlated with time.
  * instead.
  *
  * @param dataStream DStream of [[TimeRecord]] partitioned by a key.
  */
class TimeSeriesStreamPartitioned(val dataStream: DStream[(String,_ <: TimeRecord)] ) extends TimeSeriesStream {
  def getDStream: DStream[(String, _ <: TimeRecord)] = dataStream
}

object TriggerStream {
  def apply(dataStream: DStream[_ <: TriggerEvent]): TriggerStream = new TriggerStreamSimple(dataStream)
}

/**
  * Base class for all TriggerStreams. TriggerStreams are output
  * streams that contain events. Events are processed by the iobeam
  * infrastructure and trigger configurable actions to run.
  * For example, events can cause an email to be sent or
  * can cause data to be pushed to an external system like MQTT.
  *
  */
abstract class TriggerStream extends Stream {
  def getDStream: DStream[(String, _ <:TriggerEvent)]
}

/**
  * A simple TriggerStream of events.
  *
  * @param dataStream A DStream of events.
  */
private class TriggerStreamSimple (val dataStream: DStream[_ <: TriggerEvent] ) extends TriggerStream{
  def getDStream: DStream[(String, _ <: TriggerEvent)] = dataStream.map(event => (event.name, event))
}

