package com.iobeam.spark.streams.transforms


/**
  * EWMA
  */
class EwmaState(val alpha: Double) extends FieldTransformState {

    if (alpha > 1.0 || alpha < 0.0) {
        throw new IllegalArgumentException("alpha outside of 0.0 and 1.0")
    }

    var initiated = false
    var output = 0.0

    override def sampleUpdateAndTest(timeUs: Long,
                                     batchTimeUs: Long,
                                     reading: Any): Option[Any] = {

        if (!reading.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Ewma filter only accepts Double.")
        }

        val newSample = reading.asInstanceOf[Double]

        if (!initiated) {
            output = newSample
            initiated = true
            return Some(newSample)
        }

        output = output * (1.0 - alpha) + alpha * newSample
        Some(output)
    }
}

class Ewma(val alpha: Double) extends FieldTransform {
    override def getNewTransform: FieldTransformState = new EwmaState(alpha)
}