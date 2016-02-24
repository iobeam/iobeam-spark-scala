package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.OutputStreams

/**
  * A trait that defines the basic structure of a spark app.
  */
trait SparkApp extends Serializable with Logging {

    /**
      * The main function of the Spark app. This function should return
      * the processed streams as an sequence of DStreams.
      *
      * @return A set of output streams.
      */
    def main(appCtx: AppContext): OutputStreams
}
