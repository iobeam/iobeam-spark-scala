package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.model.namespaces.DataQuery
import org.apache.spark.streaming.dstream.DStream

/**
  * Context that keeps state related to a Spark app.
  */
abstract class AppContext extends Serializable {

    /**
      * Returns a stream of records from namespace namespaceName.
      * @param namespaceName namespace to read
      * @return timerecords
      */
    def getData(namespaceName: String): DStream[TimeRecord]

    /**
      * Returns records from the namespace matching query.
      * @param query set of includes and excludes
      * @return records from default input namespace matching query
      */
    def getData(query: DataQuery): DStream[TimeRecord]

}
