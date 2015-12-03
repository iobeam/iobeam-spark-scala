package com.iobeam.spark.streams.model

import org.apache.spark.streaming.dstream.DStream

/**
 * The base class representing all the outputs of a spark app.
 */
sealed abstract class Stream

/**
  * A collection of output streams representing all the results of a spark app.
  *
  * @param stream All of the output streams of an app
  */
class OutputStreams(val stream: Stream*) {
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

  /**
    * Returns the [[TimeSeriesStream]] for a given name
    * @param name
    * @return
      */
  def getTimeSeriesByName(name: String): Option[TimeSeriesStream] = getTimeSeries.find(ts => ts.name == name)
}

/**
  * Base class for all TimeSeriesStreams. These are the derived data
  * streams.
  *
  * @param name Name of the stream
  */
abstract class TimeSeriesStream(val name: String) extends Stream {
  def getDStream: DStream[(String, _ <: TimeRecord)]
}

/**
  * A TimeSeriesStream with an explicit partition key.
  * The partition key determines how the output is partioned for writing.
  * Thus, for efficiency, it should not be correlated with time.
  * If you don't know how to partition the data use a [[TimeSeriesStreamSimple]]
  * instead.
  *
  * @param name Name of the stream
  * @param dataStream DStream of [[TimeRecord]] partitioned by a key.
  */
class TimeSeriesStreamPartitioned(name: String, val dataStream: DStream[(String,_ <: TimeRecord)] ) extends TimeSeriesStream(name){
  def getDStream: DStream[(String, _ <: TimeRecord)] = dataStream
}

/**
  * A simple [[TimeSeriesStream]] that is not explicitly partitioned.
  * @param name Name of the stream
  * @param dataStream DStream of [[TimeRecord]].
  */

class TimeSeriesStreamSimple (name: String, val dataStream: DStream[_ <: TimeRecord] ) extends TimeSeriesStream(name){
  //TODO: should the "string" i.e. device id i.e. partition key be random?
  def getDStream: DStream[(String, _ <: TimeRecord)] = dataStream.map(("", _))
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

