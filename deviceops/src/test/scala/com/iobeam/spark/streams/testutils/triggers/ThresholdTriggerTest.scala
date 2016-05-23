package com.iobeam.spark.streams.testutils.triggers

import com.iobeam.spark.streams.triggers.ThresholdTrigger
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test threshold trigger
  */
class ThresholdTriggerTest extends FlatSpec with Matchers with MockitoSugar {
    val triggerEvent = "triggerEvent"
    val releaseEvent = "releaseEvent"

    "Threshold" should "trigger above threshold and below release" in {

        val threshold = new ThresholdTrigger(0.7, triggerEvent, 0.5, releaseEvent)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.7) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.6) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(None)

    }

    "Threshold" should "trigger only above threshold" in {

        val threshold = new ThresholdTrigger(0.7, triggerEvent, 0.5)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.7) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.6) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(None)

    }

    "Threshold" should "trigger below threshold and trigger above release level" in {

        val threshold = new ThresholdTrigger(0.5, triggerEvent, 0.7, releaseEvent)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.7) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(0,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.6) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(None)

    }

    "Threshold" should "trigger below threshold" in {

        val threshold = new ThresholdTrigger(0.5, triggerEvent, 0.7)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.7) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.6) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(None)

    }

    "Threshold" should "trigger above threshold and release below same level" in {

        val threshold = new ThresholdTrigger(0.5, triggerEvent, 0.5, releaseEvent)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(0,42,0.7) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(0,42,0.4) should equal(None)
    }
}
