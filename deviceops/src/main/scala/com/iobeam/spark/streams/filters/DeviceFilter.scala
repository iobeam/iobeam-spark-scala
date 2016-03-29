package com.iobeam.spark.streams.filters

import com.iobeam.spark.streams.model.TimeRecord

/**
  * Filter used when multiple time series are needed in the computation
  */

abstract class DeviceFilter extends Serializable {

    /**
      * Update filter
      * @param newSample sample value
      * @return filter output
      */
    def update(newSample: TimeRecord): Any

}
