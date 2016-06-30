package com.iobeam.spark.streams.transforms

/**
  * Define a threshold with different trigger and release levels for hysteresis.
  *
  * If triggerLevel >= releaseLevel, it triggers when series > threshold and releases when
  * series < releaseLevel
  *
  * If triggerLevel < releaseLevel, the threshold triggers when series < threshold and
  * releases when series > release.
  *
  */

class ThresholdTriggerState private[transforms](val triggerLevel: Double,
                                                val triggerEventName: String,
                                                val releaseLevel: Double,
                                                val releaseEventNameOpt: Option[String]) extends
    FieldTransformState {

    // Current trigger state, true if triggered, false if released
    private[this] var triggerState = false

    /**
      * Updates trigger state and returns trigger or None
      *
      * @param readingObj new reading
      * @return None or event name
      */

    def sampleUpdateAndTest(time: Long, batchTimeUs: Long, readingObj: Any): Option[String] = {

        if (!readingObj.isInstanceOf[Double]) {
            throw new IllegalArgumentException("Threshold trigger only accepts Double series")
        }

        val reading = readingObj.asInstanceOf[Double]
        val isTrigger = if (triggerLevel >= releaseLevel) {
            reading >= triggerLevel
        } else {
            reading <= triggerLevel
        }

        if (isTrigger) {
            if (triggerState) {
                // already triggered
                return None
            }
            triggerState = true
            return Some(triggerEventName)
        }

        // Not trigger

        val isRelease = if (triggerLevel >= releaseLevel) {
            reading <= releaseLevel
        } else {
            reading >= releaseLevel
        }

        if (isRelease) {
            if (triggerState) {
                triggerState = false
                return releaseEventNameOpt
            }
        }

        None
    }

    def isTriggered: Boolean = triggerState

    def isReleased: Boolean = !triggerState
}

class ThresholdTrigger private(val triggerLevel: Double,
                               val triggerEventName: String,
                               val releaseLevel: Double,
                               val releaseEventNameOpt: Option[String]) extends FieldTransform {

    /**
      * Threshold trigger without hysteresis.
      *
      * @param triggerLevel     readings above triggerLevel can trigger
      * @param triggerEventName name that the trigger outputs on trigger
      */

    def this(triggerLevel: Double, triggerEventName: String) {
        this(triggerLevel, triggerEventName, triggerLevel, None)
    }

    /**
      * Threshold trigger with hysteresis but without trigger on release
      *
      * If triggerLevel < releaseLevel, the treshold triggers when series < threshold and
      * releases when series > release.
      *
      * @param triggerLevel     level that readings are compared to
      * @param triggerEventName name that is output when a trigger is activated
      * @param releaseLevel     level where the trigger state is released
      */

    def this(triggerLevel: Double, triggerEventName: String, releaseLevel: Double) {
        this(triggerLevel, triggerEventName, releaseLevel, None)
    }

    /**
      * Threshold trigger with hysteresis and trigger on release as well
      *
      * @param triggerLevel     level that readings are compared to
      * @param triggerEventName name that is output when a trigger is activated
      * @param releaseLevel     level where the trigger state is released
      * @param releaseEventName name that is output when a trigger is released
      */

    def this(triggerLevel: Double, triggerEventName: String, releaseLevel: Double,
             releaseEventName: String) {
        this(triggerLevel, triggerEventName, releaseLevel, Some(releaseEventName))
    }

    def getNewTransform = new ThresholdTriggerState(triggerLevel, triggerEventName, releaseLevel,
        releaseEventNameOpt)

}