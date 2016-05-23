package com.iobeam.spark.streams.filters

/**
  * EWMA
  */
class Ewma(val alpha: Double) extends SeriesFilter {

    if (alpha > 1.0 || alpha < 0.0) {
        throw new IllegalArgumentException("alpha outside of 0.0 and 1.0")
    }

    var initiated = false
    var output = 0.0

    override def update(t: Long, batchTimeUs: Long, newSampleObj: Any): Any = {

        if (!newSampleObj.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Ewma filter only accepts Double.")
        }

        val newSample = newSampleObj.asInstanceOf[Double]

        if (!initiated) {
            output = newSample
            initiated = true
            return newSample
        }

        output = output * (1.0 - alpha) + alpha * newSample
        output
    }

    override def getValue: Any = output

}
