package com.iobeam.spark.streams.filters

/**
  * Exponential weighted moving average for irregular sampled series
  *
  * refs:
  * tdunning.blogspot.se/2011/03/exponential-weighted-averages-with.html
  * https://oroboro.com/irregular-ema/
  *
  * @param alpha weight decrease constant
  */

class EwmaIrregular(val alpha: Double) extends SeriesFilter {

    var initiated = false
    var output: Double = 0
    var prevSample: Double = 0
    var prevTimestamp: Long = 0

    override def update(t: Long, batchTimeUs: Long, newSampleObj: Any): Double = {

        if (!newSampleObj.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Ewma filter only accpets Double")
        }

        val newSample = newSampleObj.asInstanceOf[Double]
        if (t < prevTimestamp) {
            throw new IllegalArgumentException("Sample times can not be decreasing")
        }

        if (!initiated) {
            prevTimestamp = t
            output = newSample
            prevSample = newSample
            initiated = true
            return newSample
        }

        val deltaTime = t - prevTimestamp

        val a = deltaTime / alpha
        val u = Math.exp(a * -1.0)
        val v = (1.0 - u) / a

        output = (u * output) + ((v - u) * prevSample) + ((1.0 - v) * newSample)

        prevSample = newSample
        prevTimestamp = t

        output
    }

    override def getValue: Double = output
}

