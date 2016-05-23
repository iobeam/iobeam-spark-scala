package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.{TimeRecord, TriggerEvent}
import org.apache.spark.streaming.Time

/**
  * State container used by update state by key
  */
class DeviceState(val configuration: DeviceOpsConfig,
                  val deviceId: String,
                  val initializationTime: Time) extends Serializable {

    private var batchDerivedSeries = Seq[TimeRecord]()
    private var batchTriggerEvents = Seq[TriggerEvent]()
    private var lastReceivedEventTime: Long = 0
    private var batchTime = initializationTime
    /**
      * Store derived streams and trigger events from the last time slot.
      *
      * @param derivedRecords output time records from filters
      * @param triggerEvents trigger events created in the time slot
      */
    def updateState(derivedRecords: Seq[TimeRecord],
                    triggerEvents: Seq[TriggerEvent],
                    batchTime: Time,
                    lastReceivedEvent: Long): Unit = {
        batchDerivedSeries = derivedRecords
        batchTriggerEvents = triggerEvents
        lastReceivedEventTime = lastReceivedEvent
        this.batchTime = batchTime
    }

    def updateState(derivedRecords: Seq[TimeRecord],
                    triggerEvents: Seq[TriggerEvent],
                    batchTime: Time): Unit = {
        batchDerivedSeries = derivedRecords
        batchTriggerEvents = triggerEvents
        this.batchTime = batchTime
    }

    def getBatchOutputSeries: Seq[TimeRecord] = batchDerivedSeries

    def getBatchTriggerEvents: Seq[TriggerEvent] = batchTriggerEvents

    def getLastReceivedEventTime: Long = lastReceivedEventTime

    def getBatchTime = batchTime

}
