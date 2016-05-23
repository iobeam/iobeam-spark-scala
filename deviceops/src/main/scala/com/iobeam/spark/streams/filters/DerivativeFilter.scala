package com.iobeam.spark.streams.filters

/**
  * Derivate filter
  *
  * Can be used in combination with threshold triggers to detect quick changes in readings
  */
class DerivativeFilter extends SeriesFilter {
    var lastValue: Double = 0
    var lastTime: Long = 0
    var initialized = false
    var output = 0.0

    override def update(t: Long, batchTimeUs: Long, newSampleObj: Any): Any = {
        if (!newSampleObj.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Derivative filter only accepts Double")
        }

        val newSample = newSampleObj.asInstanceOf[Double]

        if (!initialized) {
            lastValue = newSample
            lastTime = t
            initialized = true
            return 0.0
        }

        val deltaY = newSample - lastValue
        val deltaT = t - lastTime

        lastTime = t
        lastValue = newSample

        deltaY / deltaT.toDouble
    }

    override def getValue: Any = output
}
