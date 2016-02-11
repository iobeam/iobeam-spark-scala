package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.logging.log4j.LogManager
import org.apache.spark.streaming.dstream.DStream

/**
  * Interface to run iobeam spark apps locally.
  */
class AppRunnerInterface(stream: DStream[(String, TimeRecord)]) extends IobeamInterface {
    /**
      * Logger for user app log ouput
      */
    override def getLogger = LogManager.getLogger(this.getClass)

    /**
      * Returns a stream of RDDs of key, TimeRecord where key is the RDD partitioning key
      */
    override def getInputStreamBySource: DStream[(String, TimeRecord)] = stream

    /**
      * Returns a stream of TimeRecords
      */
    override def getInputStreamRecords: DStream[TimeRecord] = stream.map(a => a._2)
}
