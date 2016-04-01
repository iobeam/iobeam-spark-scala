package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.{TriggerEvent, TimeRecord}

/**
  * State container used by update state by key
  */
class DeviceState(val configuration: DeviceOpsConfig,
                  val deviceId: String) extends Serializable {

    private var batchDerivedSeries = Seq[TimeRecord]()
    private var batchTriggerEvents = Seq[TriggerEvent]()

    /**
      * Store derived streams and trigger events from the last time slot.
      *
      * @param derivedRecords output time records from filters
      * @param triggerEvents trigger events created in the time slot
      */
    def updateState(derivedRecords: Seq[TimeRecord], triggerEvents: Seq[TriggerEvent]): Unit = {
        batchDerivedSeries = derivedRecords
        batchTriggerEvents = triggerEvents
    }

    def getBatchOutputSeries: Seq[TimeRecord] = batchDerivedSeries

    def getBatchTriggerEvents: Seq[TriggerEvent] = batchTriggerEvents
}
