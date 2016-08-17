package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.streaming.Time

/**
  * State container used by update state by key
  */
class DeviceState(val state: DeviceOpsState,
                  val deviceId: String,
                  val initializationTime: Time) extends Serializable {

    private var batchDerivedSeries = Seq[TimeRecord]()
    private var lastReceivedEventTime: Long = 0
    private var batchTime = initializationTime

    /**
      * Store derived streams and trigger events from the last time slot.
      *
      * @param derivedRecords output time records from filters
      */
    def updateState(derivedRecords: Seq[TimeRecord],
                    batchTime: Time,
                    lastReceivedEvent: Long): Unit = {
        batchDerivedSeries = derivedRecords
        lastReceivedEventTime = lastReceivedEvent
        this.batchTime = batchTime
    }

    def updateState(derivedRecords: Seq[TimeRecord],
                    batchTime: Time): Unit = {
        batchDerivedSeries = derivedRecords
        this.batchTime = batchTime
    }

    def getBatchOutputSeries: Seq[TimeRecord] = batchDerivedSeries

    def getLastReceivedEventTime: Long = lastReceivedEventTime

    def getBatchTime = batchTime

}
