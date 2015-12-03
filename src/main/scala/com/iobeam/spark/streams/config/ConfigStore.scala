package com.iobeam.spark.streams.config

/**
  * Stores configuration objects on a per-device basis.
  *
  * @param configs Map from device IDs to DeviceConfigs.
  */

class ConfigStore(private val configs: Map[String, DeviceConfig] = Map())
    extends Serializable {

    def this(configs: Seq[(String, DeviceConfig)]) = this(configs.toMap)

    def toMap: Map[String, DeviceConfig] = configs

    def toSeq: Seq[(String, DeviceConfig)] = configs.toSeq

  /**
    * Update with a new DeviceConfig
    *
    * @param deviceConfig The ConfigStore update.
    * @return The updated ConfigStore.
    */

    def update(deviceConfig: DeviceConfig): ConfigStore = {

        val oldDeviceConfig = this.configs.get(deviceConfig.deviceId)

        if (oldDeviceConfig.isDefined) {
            new ConfigStore(this.configs ++ Map(deviceConfig.deviceId ->
                new DeviceConfig(deviceConfig.deviceId,
                    oldDeviceConfig.get.timeSeriesConfigs
                        ++
                        deviceConfig.timeSeriesConfigs)))
        } else {
            new ConfigStore(this.configs ++ Map(deviceConfig.deviceId -> deviceConfig))
        }
    }

  /**
    * Returns the [[DeviceConfig]] for a device.
    *
    * Resolve the DeviceConfig in a cascading manner:
    *  1. Return the device-specific configuration, if it exists. Else:
    *  1. Return the default configuration for all devices, if it exists. Else:
    *  1. Return an empty DeviceConfig
    *
    * @param deviceId The ID of the device.
    */
    def get(deviceId: String): DeviceConfig = {
        configs.get(deviceId)
            .orElse(configs.get("*"))
            .getOrElse(new DeviceConfig("*"))
    }

  /**
    * Returns the [[SeriesConfig]] for a device and time-series.
    *
    * @param deviceId ID of the device.
    * @param timeSeriesName Name of the timeseries
    */
    def get(deviceId: String, timeSeriesName: String): SeriesConfig = {
        get(deviceId).get(timeSeriesName)
    }

    override def toString: String = s"ConfigStore($configs)"
}
