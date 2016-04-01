package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.dstream.DStream

/**
  * Context that keeps state related to a Spark app.
  */
abstract class AppContext extends Serializable {

    /**
      * Returns a stream of RDDs of key, TimeRecord where key is the deviceId
      */
    def getInputStream: DStream[(String, TimeRecord)]
}
