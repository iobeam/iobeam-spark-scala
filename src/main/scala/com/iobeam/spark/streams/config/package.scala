package com.iobeam.spark.streams

/**
  * Used to modify the configuration of an iobeam spark app at runtime. Typically information
  * include user configurable thresholds and device specific calibration. The configuration is
  * updated through the iobeam configs API and can be accessed in the iobeam spark app.
  * Configurations are simple maps of parameter keys to values. Configurations can apply
  * to all apps or can be specific to a time-series or a device. A SeriesConfig holds all
  * config values for a time-series while a DeviceConfig holds a collection of SeriesConfigs
  * for a particular device. The spark app gets a DStream of data joined
  * with the configuration. The configuration is resolved on the time-series and
  * device of the data to be the most specific configuration appropriate for the data.
  *
  */
package object config
