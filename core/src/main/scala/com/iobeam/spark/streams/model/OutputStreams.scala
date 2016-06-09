package com.iobeam.spark.streams.model

import com.iobeam.spark.streams.model.OutputStreams.TimeRecordDStream
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

import scala.language.implicitConversions

/**
  * A collection of output streams representing all the results of a spark app.
  */
class OutputStreams private(val streams: Seq[TimeRecordDStream]) extends
    Iterable[DStream[TimeRecord]] {

    def getDStreams: Seq[TimeRecordDStream] = streams

    override def iterator: Iterator[DStream[TimeRecord]] = streams.iterator
}

object OutputStreams {

    class TimeRecordDStream(namespaceName: String, parent: DStream[TimeRecord])
        extends DStream[TimeRecord](parent.context) {
        override def slideDuration: Duration = parent.slideDuration

        override def dependencies: List[DStream[_]] = List(parent)

        override def compute(validTime: Time): Option[RDD[TimeRecord]] =
            parent.compute(validTime)

        def getNamespaceName: String = namespaceName

        def unWrap: DStream[TimeRecord] = parent
    }

    implicit def toTimeRecordDStream(stream: (String, DStream[TimeRecord])): TimeRecordDStream =
        new TimeRecordDStream(stream._1, stream._2)

    def apply(timeRecordStream: TimeRecordDStream*) =
        new OutputStreams(Seq(timeRecordStream: _*))

}