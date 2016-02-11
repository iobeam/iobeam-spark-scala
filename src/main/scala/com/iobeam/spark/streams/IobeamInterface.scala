package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.logging.log4j
import org.apache.spark.streaming.dstream.DStream

/**
  * Connections to iobeam backend.
  */

abstract class IobeamInterface extends Serializable {
    /**
      * Logger for user app log ouput
      */
    def getLogger: log4j.Logger

    /**
      * Returns a stream of RDDs of key, TimeRecord where key is the RDD partitioning key
      */
    def getInputStreamBySource: DStream[(String, TimeRecord)]

    /**
      * Returns a stream of TimeRecords
      */
    def getInputStreamRecords: DStream[TimeRecord]

}
