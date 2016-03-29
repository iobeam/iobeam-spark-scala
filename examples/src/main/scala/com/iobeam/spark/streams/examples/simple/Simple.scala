package com.iobeam.spark.streams.examples.simple

import com.iobeam.spark.streams.annotation.SparkRun
import com.iobeam.spark.streams.model.{OutputStreams, TimeRecord}
import com.iobeam.spark.streams.{AppContext, SparkApp}

/**
  * Adds 10 to the field value.
  */
@SparkRun("Simple")
object Simple extends SparkApp {

    override def main(appContext: AppContext):
    OutputStreams = {
        val stream = appContext.getInputStream
        val derivedStream = stream.mapValues {
            ds: TimeRecord => {
                val oldValue = ds.requireDouble("value")
                val data = Map[String, Any]("value-new" -> (oldValue + 10))
                new TimeRecord(ds.time, data)
            }
        }

        OutputStreams(derivedStream)
    }
}
