package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.OutputStreams.TimeRecordDStream
import com.iobeam.spark.streams.model.{OutputStreams, TimeRecord}
import org.apache.spark.SparkEnv
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Time}

import scala.collection.mutable.ListBuffer

/**
  * State update module.
  */

object DeviceOps {

    private def applyFieldTransforms(records: Seq[(TimeRecord, Time)],
                                     deviceState: DeviceState): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]

        for ((record, time) <- records) {
            for ((field, reading) <- record.getData) {
                for (transformConf <-
                     deviceState.state.getFieldsTransforms
                         .getOrElse(field, Seq())) {
                    val outputField = transformConf.outputField
                    val outputVal = transformConf.transform
                        .sampleUpdateAndTest(record.time, time.milliseconds * 1000, reading)

                    if (outputVal.isDefined) {
                        val output = new TimeRecord(record.time,
                            Map(deviceState.deviceIdField -> deviceState.deviceId,
                                outputField -> outputVal.get))

                        listBuilder.append(output)
                    }
                }
            }
        }

        listBuilder
    }


    private def applyFieldTransformsBatchEnd(timeUs: Long,
                                             deviceState: DeviceState): Seq[TimeRecord] = {
        val listBuilder = new ListBuffer[TimeRecord]
        for (transforms <- deviceState.state.getFieldsTransforms.values) {
            for (transformConf <- transforms) {
                val outputVal = transformConf.transform
                    .batchDoneUpdateAndTest(timeUs)
                if (outputVal.isDefined) {
                    val output = new TimeRecord(timeUs,
                        Map(deviceState.deviceIdField -> deviceState.deviceId,
                            transformConf.outputField -> outputVal.get))

                    listBuilder.append(output)
                }
            }
        }

        listBuilder
    }

    private def applyNamespaceTransforms(records: Seq[(TimeRecord, Time)],
                                         deviceState: DeviceState): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]
        val transforms = deviceState.state.getNamespaceTransforms

        for ((record, time) <- records) {

            for ((outputField, transform) <- transforms) {

                val timeUs = time.milliseconds * 1000
                val outputVal = transform.recordUpdateAndTest(record, timeUs)
                if (outputVal.isDefined) {
                    listBuilder.append(new TimeRecord(record.time,
                        Map(deviceState.deviceIdField -> deviceState.deviceId,
                            outputField -> outputVal.get)))
                }
            }
        }

        listBuilder
    }

    private def applyNamespaceTransformsBatchEnd(timeUs: Long,
                                         deviceState: DeviceState): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]
        val transforms = deviceState.state.getNamespaceTransforms

        for ((outputField, transform) <- transforms) {

                val outputVal = transform.batchDoneUpdateAndTest(timeUs)
                if (outputVal.isDefined) {
                    listBuilder.append(new TimeRecord(timeUs,
                        Map(deviceState.deviceIdField -> deviceState.deviceId,
                            outputField -> outputVal.get)))
                }
            }

        listBuilder
    }

    /**
      * Processes a time slot of time records by sorting them and processing
      * them with configured transforms. Used by updateStateByKey.
      *
      * @param recordsWithConfAndDevice unsorted records in a time slot
      * @param state                    state kept from last time slot
      * @return state from this time slot
      */
    def processTimeSlot(recordsWithConfAndDevice: Seq[(TimeRecord, DeviceOpsConfig,
        String, Time)], state: Option[DeviceState]): Option[DeviceState] = {

        // Contains the output time series
        val transformedRecords = new ListBuffer[TimeRecord]

        var deviceState: DeviceState = null

        if (state.isEmpty) {
            // It is the first we have seen from the device,
            // initiate state.

            if (recordsWithConfAndDevice.isEmpty) {
                // Should not happen
                return None
            }

            val (_, deviceConf, deviceId, batchTimeUs) = recordsWithConfAndDevice.head
            // Build a new set of initiated transforms
            val deviceTransforms = deviceConf.build
            deviceState = new DeviceState(deviceTransforms,
                deviceConf.deviceField, deviceId, batchTimeUs)

        } else {
            deviceState = state.get
        }

        // drop conf and device id and reordered data
        val records = recordsWithConfAndDevice
            .map(a => (a._1, a._4))
            .filter(a => a._1.time > deviceState.getLastReceivedEventTime)

        // make sure that the batch is sorted before processing
        val sortedBatch = records.toList.sortBy(a => a._1.time)

        val batchTimeStamp = if (records.isEmpty) {
            deviceState.getBatchTime + Seconds(SparkEnv.get.conf.get("spark.batch.duration.s",
                "0").toInt)
        } else {
            val (_, _, _, batchTime) = recordsWithConfAndDevice.head
            batchTime
        }

        // apply namespace transforms
        transformedRecords ++= applyNamespaceTransforms(sortedBatch, deviceState)

        // apply field filters to the new Data and the output from previous batch
        transformedRecords ++= applyFieldTransforms(sortedBatch ++ deviceState.getBatchOutputSeries
            .map(a => (a, batchTimeStamp)), deviceState)

        val nowUs: Long = batchTimeStamp.milliseconds * 1000

        transformedRecords ++= applyNamespaceTransformsBatchEnd(nowUs, deviceState)
        transformedRecords ++= applyFieldTransformsBatchEnd(nowUs, deviceState)

        if (sortedBatch.isEmpty) {
            deviceState.updateState(transformedRecords, batchTimeStamp)
        } else {
            deviceState.updateState(transformedRecords, batchTimeStamp, sortedBatch.last._1.time)
        }
        Some(deviceState)
    }

    private def setupMonitoring(batches: DStream[(String, TimeRecord)],
                                monitoringConfiguration: DeviceOpsConfig): DStream[TimeRecord] = {

        // Join in configuration and deviceId's to be used in calculation,
        // then use updateStateByKey where key is deviceId
        val stateStream = batches.transform(
            (rdd, time) => rdd.map(a => (a._1, (a._2, monitoringConfiguration, a._1, time))))
            .updateStateByKey(DeviceOps.processTimeSlot)

        stateStream.flatMap(a => a._2.getBatchOutputSeries)
    }

    /**
      * Setup device monitoring stream processing and get series and trigger events.
      *
      * @param batches                 time records
      * @param monitoringConfiguration filters and triggers configuration.
      * @return tuple of output series and trigger events
      */
    def getDeviceOpsOutput(batches: DStream[TimeRecord],
                           deviceField: String,
                           monitoringConfiguration: DeviceOpsConfig): TimeRecordDStream = {
        val keyBatches = batches.map({ case (tr: TimeRecord) => (tr.requireString(deviceField),
            tr) })

        (monitoringConfiguration.getWriteNamespace, deviceField,
            setupMonitoring(keyBatches, monitoringConfiguration))
    }

    /**
      * Setup device monitoring stream processing and get series and trigger events.
      *
      * @param batches time records
      * @return tuple of output series and trigger events
      */
    def getDeviceOpsOutput(batches: DStream[TimeRecord],
                           monitoringConfiguration: DeviceOpsConfig): TimeRecordDStream = {
        val keyBatches = batches.map({ case (tr: TimeRecord) => (tr.requireString
        (monitoringConfiguration.deviceField), tr) })

        (monitoringConfiguration.getWriteNamespace, monitoringConfiguration.deviceField,
            setupMonitoring(keyBatches, monitoringConfiguration))
    }

    /**
      * Setup device monitoring stream processing and get OutputStreams.
      *
      * @param batches                 time records
      * @param deviceField             namespace field that identifies a device
      * @param monitoringConfiguration filters and triggers configuration.
      * @return OutputStreams
      */
    def monitorDevices(batches: DStream[TimeRecord],
                       deviceField: String,
                       monitoringConfiguration: DeviceOpsConfig): OutputStreams = {
        val keyBatches = batches.map({ case (tr: TimeRecord) => (tr.requireString(deviceField),
            tr) })

        val series = setupMonitoring(keyBatches, monitoringConfiguration)
        OutputStreams((monitoringConfiguration.getWriteNamespace, deviceField, series))
    }

    /**
      * Setup device monitoring stream processing and get OutputStreams.
      *
      * @param batches                 time records
      * @param monitoringConfiguration filters and triggers configuration.
      * @return OutputStreams
      */
    def monitorDevices(batches: DStream[TimeRecord],
                       monitoringConfiguration: DeviceOpsConfig): OutputStreams = {
        monitorDevices(batches, monitoringConfiguration.deviceField, monitoringConfiguration)
    }
}
