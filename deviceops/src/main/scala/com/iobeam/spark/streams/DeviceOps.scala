package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.OutputStreams.{TriggerEventDStream, TimeSeriesDStream}
import com.iobeam.spark.streams.model.{OutputStreams, TriggerEvent, TimeRecord}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer

/**
  *
  */

object DeviceOps {

    private def applyDeviceTriggers(records: Seq[TimeRecord],
                                    deviceState: DeviceState): Seq[TriggerEvent] = {

        val deviceTriggers = new ListBuffer[TriggerEvent]

        for (record <- records) {
            for (trigger <- deviceState.configuration.getDeviceTriggers) {

                val triggerEvent = trigger.recordUpdateAndTest(record)
                if (triggerEvent.isDefined) {
                    deviceTriggers.append(new TriggerEvent(triggerEvent.get,
                        record ++ Map("deviceID" -> deviceState.deviceId)))
                }

            }
        }

        deviceTriggers.toSeq
    }

    private def applySeriesTriggers(records: Seq[TimeRecord], deviceState: DeviceState): Seq[TriggerEvent] = {

        val triggerEvents = new ListBuffer[TriggerEvent]
        val triggers = deviceState.configuration.getSeriesTriggers

        for (record <- records) {
            for ((name, reading) <- record.getData) {

                // Check all triggers configured for the series
                for (trigger <- triggers.getOrElse(name, Seq())) {
                    val triggerEvent = trigger.sampleUpdateAndTest(record.time, reading)

                    if (triggerEvent.isDefined) {
                        triggerEvents.append(new TriggerEvent(triggerEvent.get, record ++
                            Map("deviceId" -> deviceState.deviceId)))
                    }
                }
            }
        }
        triggerEvents.toSeq
    }

    private def applySeriesFilters(records: Seq[TimeRecord],
                                   deviceState: DeviceState): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]

        // apply series on all new records and the derived records from the last batch
        for (record <- records) {
            for ((series, reading) <- record.getData) {
                for (filterConf <- deviceState.configuration.getSeriesFilters.getOrElse(series, Seq())) {
                    val output = new TimeRecord(record.time, Map(filterConf.outputSeries ->
                        filterConf.filter.update(record.time, reading)))
                    listBuilder.append(output)
                }
            }
        }

        listBuilder.toSeq
    }

    private def applyDeviceFilters(records: Seq[TimeRecord],
                                   deviceState: DeviceState): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]
        for (record <- records) {
            for (filterConf <- deviceState.configuration.getDeviceFilters) {
                listBuilder.append(new TimeRecord(record.time,
                    Map(filterConf.outputSeries ->
                        filterConf.filter.update(record))))
            }
        }

        listBuilder.toSeq
    }


    /**
      * Processes a time slot of time records by sorting them and processing them with configured
      * filters and triggers. Used by updateStateByKey.
      *
      * @param recordsWithConfAndDevice unsorted records in a time slot
      * @param state state kept from last time slot
      * @return state from this time slot
      */
    def processTimeSlot(recordsWithConfAndDevice: Seq[(TimeRecord, DeviceOpsConfig,
        String)],state: Option[DeviceState]): Option[DeviceState] = {

        // Contains the output time series
        val filteredRecords = new ListBuffer[TimeRecord]
        val triggerRecords = new ListBuffer[TriggerEvent]

        var deviceState: DeviceState = null

        if (state.isEmpty) {
            // It is the first we have seen from the device,
            // initiate state.

            if(recordsWithConfAndDevice.isEmpty) {
                // Should not happen
                return None
            }

            val (_, deviceConf, deviceId) = recordsWithConfAndDevice.head
            deviceState = new DeviceState(deviceConf, deviceId)

        } else {
            deviceState = state.get
        }

        // drop conf and device id
        val records = recordsWithConfAndDevice.map(a => a._1)
        // make sure that the batch is sorted before processing
        val sortedBatch = records.toList.sortBy(tr => tr.time)

        triggerRecords ++= applyDeviceTriggers(sortedBatch, deviceState)
        filteredRecords ++= applyDeviceFilters(sortedBatch, deviceState)

        // apply series filters to the new Data and the output from previous batch
        filteredRecords ++= applySeriesFilters(sortedBatch ++
            deviceState.getBatchOutputSeries,
            deviceState)

        // Update and Check triggers on both raw series and processed series
        triggerRecords ++= applySeriesTriggers(sortedBatch ++ filteredRecords, deviceState)

        val nowUs = (System.currentTimeMillis * 1000.0).toLong

        for (trigger <- deviceState.configuration.getDeviceTriggers) {
            val triggerName = trigger.batchDoneUpdateAndTest(nowUs)
            if (triggerName.isDefined) {
                triggerRecords.append(new TriggerEvent(triggerName.get, new TimeRecord(nowUs, Map
                ("deviceId" -> deviceState.deviceId))))
            }
        }

        deviceState.updateState(filteredRecords, triggerRecords)
        Some(deviceState)
    }

    private def setupMonitoring(batches: DStream[(String, TimeRecord)],
                                monitoringConfiguration: DeviceOpsConfig): (TimeSeriesDStream, TriggerEventDStream) = {

        // Join in configuration and deviceId's to be used in calculation,
        // then use updateStateByKey where key is deviceId
        val stateStream = batches.transform(
            rdd => rdd.map(a => (a._1, (a._2, monitoringConfiguration, a._1))))
            .updateStateByKey(DeviceOps.processTimeSlot)

        // Get an output stream of time records from the deviceState objects
        val seriesStreamPartitioned = stateStream.flatMapValues(state => state.getBatchOutputSeries)

        // Get an output stream of triggers from the device state
        val triggerEvents = stateStream.flatMapValues(state => state.getBatchTriggerEvents)

        // make time series of trigger series for debugging
        val triggerSeries = triggerEvents.mapValues(
            a => new TimeRecord(a.data.time, Map(a.name -> 1.0)))

        // join filtered series with the debug trigger series
        val outSeries = seriesStreamPartitioned.union(triggerSeries)

        // Return the series and the triggers with the deviceId stripped.
        (outSeries, triggerEvents.map(a => a._2))
    }

    /**
      * Setup device monitoring stream processing and get series and trigger events.
      *
      * @param batches time records
      * @param monitoringConfiguration filters and triggers configuration.
      * @return tuple of output series and trigger events
      */
    def getDeviceOpsOutput(batches: DStream[(String, TimeRecord)],
                                  monitoringConfiguration: DeviceOpsConfig): (TimeSeriesDStream, TriggerEventDStream) = {
        setupMonitoring(batches, monitoringConfiguration)
    }

    /**
      * Setup device monitoring stream processing and get OutputStreams.
      *
      * @param batches time records
      * @param monitoringConfiguration filters and triggers configuration.
      * @return OutputStreams
      */
    def monitorDevices(batches: DStream[(String, TimeRecord)],
                       monitoringConfiguration: DeviceOpsConfig): OutputStreams = {
        val (series, triggers) = setupMonitoring(batches, monitoringConfiguration)
        OutputStreams(series, triggers)
    }
}
