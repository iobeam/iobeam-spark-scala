package com.iobeam.spark.streams.config

import java.util.UUID

import com.iobeam.spark.streams.util.AppUtil

object SeriesConfig{
  val DEFAULT_SERIES_NAME = "*"
}
/**
  * Configuration for a time-series.
  * Objects of this class are contained in [[DeviceConfig]]s and thus
  * apply to particular devices (or the * default device).
  *
  * @constructor Create a new object for a particular device and time-series.
  * @param deviceId ID of a specific device.
  * @param timeSeriesName Name of a specific time-series.
  * @param parameters Map or parameter names to values.
  */
case class SeriesConfig(deviceId: String,
                        timeSeriesName: String,
                        parameters: Map[String, Any] = Map())
  extends Serializable {

  /**
    * Creates a new default(empty) configuration object for the
    * default device and time-series.
    */
  def this() = this(DeviceConfig.DEFAULT_DEVICE_NAME, SeriesConfig.DEFAULT_SERIES_NAME, Map())

  /**
    * Creates an object with a single parameter.
    *
    * @param deviceId ID of a specific device.
    * @param timeSeriesName Name of a specific time-series.
    * @param parameterName  Name of the parameter
    * @param parameterValue Value of the parameter
    */
  def this(deviceId: String, timeSeriesName: String, parameterName: String, parameterValue: Any) =
    this(deviceId, timeSeriesName, Map(parameterName->parameterValue))

  /**
    * Returns the parameter value for a given parameter.
    *
    * @param name parameter name.
    */
  def get(name: String): Option[Any] = parameters.get(name)

  //TODO: remove this from library?
  /* Non scaladoc
    * Get an UUID for the SeriesConfig. Used by iobeam internals.
    *
    * @param projectId The ID of the iobeam project.
    * @return The UUID
    */
  def getUuid(projectId: Long): UUID = AppUtil.createConfigUuid(projectId, deviceId, timeSeriesName)

  /**
    * Returns the device ID of the associated device.
    */
  def getDeviceId: String = deviceId

  /**
    * Returns if this is default configuration for all devices.
    */
  def isDefault: Boolean = deviceId == DeviceConfig.DEFAULT_DEVICE_NAME

  override def toString: String = s"TimeSeriesConfig($timeSeriesName, $parameters)"
}
