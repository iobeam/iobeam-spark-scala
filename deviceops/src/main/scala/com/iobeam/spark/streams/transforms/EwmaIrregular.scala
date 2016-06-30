package com.iobeam.spark.streams.transforms

/**
  * Exponential weighted moving average for irregular sampled series
  *
  * refs:
  * tdunning.blogspot.se/2011/03/exponential-weighted-averages-with.html
  * https://oroboro.com/irregular-ema/
  *
  * @param alpha weight decrease constant
  */

class EwmaIrregularState(val alpha: Double) extends FieldTransformState {

    var initiated = false
    var output: Double = 0
    var prevSample: Double = 0
    var prevTimestamp: Long = 0


    override def sampleUpdateAndTest(timeUs: Long,
                                     batchTimeUs: Long,
                                     reading: Any): Option[Any] = {

        if (!reading.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Ewma filter only accepts Double")
        }

        val newSample = reading.asInstanceOf[Double]
        if (timeUs < prevTimestamp) {
            throw new IllegalArgumentException("Sample times can not be decreasing")
        }

        if (!initiated) {
            prevTimestamp = timeUs
            output = newSample
            prevSample = newSample
            initiated = true
            return Some(newSample)
        }

        val deltaTime = timeUs - prevTimestamp

        val a = deltaTime / alpha
        val u = Math.exp(a * -1.0)
        val v = (1.0 - u) / a

        output = (u * output) + ((v - u) * prevSample) + ((1.0 - v) * newSample)

        prevSample = newSample
        prevTimestamp = timeUs

        Some(output)
    }
}

class EwmaIrregular(val alpha: Double) extends FieldTransform {
    override def getNewTransform: FieldTransformState = new EwmaIrregularState(alpha)
}
