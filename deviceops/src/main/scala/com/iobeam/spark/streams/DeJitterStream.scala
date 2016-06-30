package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

/**
  * Collect and emit data in batch order that is received out of order.
  *
  * dejitter-buffer
  */

object DeJitterStream {

    def getTimeSlot(stream: DStream[(String, TimeRecord)],
                    windowLength: Duration,
                    windowSlide: Duration): DStream[(String, TimeRecord)] = {
        val inputWindow = stream.window(windowLength, windowSlide)

        // Remove samples that are to old
        val window = inputWindow.transform(
            (rdd, time) => rdd.filter(_._2.time > (time - windowLength).milliseconds)
        )

        // Get windowSlide time slot of the oldest data
        window.transform(
            (rdd, time) =>
                rdd.filter(_._2.time < (time - (windowLength - windowSlide)).milliseconds)
        )

    }
}
