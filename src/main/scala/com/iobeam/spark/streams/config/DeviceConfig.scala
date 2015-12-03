package com.iobeam.spark.streams.config

object DeviceConfig {
    val DEFAULT_DEVICE_NAME = "*"
}
/**
  * The configuration for a device, as a collection of time-series configurations.
  * A collection of all [[SeriesConfig]] for a device.
  *
  * @constructor Creates a [[DeviceConfig]] for a specific device
  * @param deviceId ID of the device
  * @param timeSeriesConfigs Map of time-series name to [[SeriesConfig]]
  */
class DeviceConfig(val deviceId: String,
                   val timeSeriesConfigs: Map[String, SeriesConfig] = Map())
    extends Serializable {

    /**
      * Creates default [[DeviceConfig]] which applies to all devices.
      */
    def this() = this(DeviceConfig.DEFAULT_DEVICE_NAME, Map())

    /**
      * Creates a [[DeviceConfig]] for a specific device
      * with a single time-series containing a single parameter.
      *
      * @param deviceId ID of associated device
      * @param timeSeriesName Name of the time-series
      * @param parameterName Name of the parameter
      * @param parameterValue Value of the parameter
      */
    def this(deviceId: String, timeSeriesName: String, parameterName: String, parameterValue: Any) =
        this(deviceId, Map(timeSeriesName ->
            new SeriesConfig(deviceId, timeSeriesName, parameterName, parameterValue)))


    /**
      * Returns the [[SeriesConfig]] for a time-series.
      *
      * Cascading resolution:
      *  1. Return the time-series-specific [[SeriesConfig]], if it exists. Else:
      *  1. Return the default [[SeriesConfig]] set for all time-series, if it exists. Else:
      *  1. Return an empty [[SeriesConfig]]
      *
      * @param timeSeriesName Name of the time-series.
      */
    def get(timeSeriesName: String): SeriesConfig =
        timeSeriesConfigs.get(timeSeriesName)
            .orElse(timeSeriesConfigs.get(DeviceConfig.DEFAULT_DEVICE_NAME))
            .getOrElse(new SeriesConfig)

    /**
      * Returns the parameter value from a given time-series and parameter name.
      * Follows the cascading rules in [[DeviceConfig]] and [[SeriesConfig]].
      *
      * @param timeSeriesName Name of the time series
      * @param parameterName Name of the parameter
      */

    def get(timeSeriesName: String, parameterName: String): Option[Any] = {
        get(timeSeriesName).get(parameterName)
    }

    /**
      * Returns whether this DeviceConfig is the default one or specific to a device.
      */

    def isDefault: Boolean = deviceId == DeviceConfig.DEFAULT_DEVICE_NAME

    override def toString: String = s"DeviceConfig($deviceId, $timeSeriesConfigs)"
}
