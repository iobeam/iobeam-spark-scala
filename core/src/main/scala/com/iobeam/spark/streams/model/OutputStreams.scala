package com.iobeam.spark.streams.model

import com.iobeam.spark.streams.model.OutputStreams.{TimeSeriesDStream, TriggerEventDStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

import scala.language.implicitConversions

/**
  * A collection of output streams representing all the results of a spark app.
  */
class OutputStreams private(val streams: Seq[DStream[_]]) extends Iterable[DStream[_]] {

    // Allow both orders of args to not break when we go back to varargs
    def this(triggerEventStream: TriggerEventDStream,
             timeSeriesStream: TimeSeriesDStream) {
        this(Seq(timeSeriesStream, triggerEventStream))
    }

    def this(timeSeriesStream: TimeSeriesDStream,
             triggerEventStream: TriggerEventDStream) {
        this(Seq(timeSeriesStream, triggerEventStream))
    }

    def this(timeSeriesStream: TimeSeriesDStream) {
        this(Seq(timeSeriesStream))
    }

    def this(triggerEventStream: TriggerEventDStream) {
        this(Seq(triggerEventStream))
    }

    /**
      * Returns all of the time series DStreams.
      *
      * @return
      */
    def getTimeSeriesDStreams: Seq[DStream[(String, TimeRecord)]] = streams.flatMap {
        case t: TimeSeriesDStream => Seq(t)
        case _ => Seq()
    }

    /**
      * Return all of the trigger event DStreams.
      *
      * @return
      */
    def getTriggerSeriesDStreams: Seq[DStream[(String, TriggerEvent)]] = streams.flatMap {
        case t: TriggerEventDStream => Seq(t)
        case _ => Seq()
    }

    /**
      * Return all the DStreams.
      *
      * @return
      */
    def getDStreams: Seq[DStream[_]] = streams

    override def iterator: Iterator[DStream[_]] = streams.iterator
}

object OutputStreams {

    class TimeSeriesDStream(parent: DStream[(String, TimeRecord)])
        extends DStream[(String, TimeRecord)](parent.context) {
        override def slideDuration: Duration = parent.slideDuration

        override def dependencies: List[DStream[_]] = List(parent)

        override def compute(validTime: Time): Option[RDD[(String, TimeRecord)]] =
            parent.compute(validTime)

        def unWrap: DStream[(String, TimeRecord)] = parent
    }

    // Implicitly converts a Pair DStream of TimeRecords to a TimeSeriesDStream.
    implicit def toTimeSeriesDStream(stream: DStream[(String, TimeRecord)]): TimeSeriesDStream =
        new TimeSeriesDStream(stream)

    class TriggerEventDStream(parent: DStream[(String, TriggerEvent)])
        extends DStream[(String, TriggerEvent)](parent.context) {
        override def slideDuration: Duration = parent.slideDuration

        override def dependencies: List[DStream[_]] = List(parent)

        override def compute(validTime: Time): Option[RDD[(String, TriggerEvent)]] =
            parent.compute(validTime)

        def unWrap: DStream[(String, TriggerEvent)] = parent
    }

    // Implicitly converts a Pair DStream of TriggerEvents to a TriggerEventDStream.
    implicit def toTriggerEventDStreamFromPair(stream: DStream[(String, TriggerEvent)]): TriggerEventDStream =
        new TriggerEventDStream(stream)

    // Implicitly converts a DStream of TriggerEvents to a TriggerEventDStream.
    implicit def toTriggerEventDStream(stream: DStream[TriggerEvent]): TriggerEventDStream =
        new TriggerEventDStream(stream.map(event => (event.name, event)))

    def apply(triggerEventStream: TriggerEventDStream, timeSeriesStream: TimeSeriesDStream) =
        new OutputStreams(triggerEventStream, timeSeriesStream)

    def apply(timeSeriesStream: TimeSeriesDStream, triggerEventStream: TriggerEventDStream) =
        new OutputStreams(timeSeriesStream, triggerEventStream)

    def apply(timeSeriesStream: TimeSeriesDStream) = new OutputStreams(timeSeriesStream)

    def apply(triggerEventStream: TriggerEventDStream) = new OutputStreams(triggerEventStream)
}
