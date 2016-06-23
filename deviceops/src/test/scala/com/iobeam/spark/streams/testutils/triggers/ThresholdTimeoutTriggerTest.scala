package com.iobeam.spark.streams.testutils.triggers

import com.iobeam.spark.streams.triggers.ThresholdTimeoutTrigger
import org.apache.spark.streaming.Milliseconds
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test timeout trigger
  */
class ThresholdTimeoutTriggerTest extends FlatSpec with Matchers with MockitoSugar {

    val triggerEvent = "triggerEvent"
    val releaseEvent = "releaseEvent"

    "Threshold" should "trigger when value has been above threshold for longer than timeout" in {

        val threshold = new ThresholdTimeoutTrigger(0.7, 0.5, Milliseconds(100), triggerEvent, releaseEvent)

        threshold.sampleUpdateAndTest(0,42,0.2) should equal(None)
        threshold.sampleUpdateAndTest(1000,42,0.5) should equal(None)
        threshold.sampleUpdateAndTest(2000,42,0.7) should equal(None)
        threshold.sampleUpdateAndTest(3000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(4000,42,0.3) should equal(None)
        threshold.sampleUpdateAndTest(40000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(80000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(150000,42,0.8) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(160000,42,0.5) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(170000,42,0.4) should equal(None)
        threshold.sampleUpdateAndTest(180000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(190000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(200000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(300000,42,0.8) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(360000,42,0.5) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(370000,42,0.4) should equal(None)

    }


    "Threshold" should "trigger when value has been above threshold for longer than timeout and start above threshold" in {

        val threshold = new ThresholdTimeoutTrigger(0.7, 0.5, Milliseconds(100), triggerEvent, releaseEvent)

        threshold.sampleUpdateAndTest(0,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(1000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(2000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(3000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(4000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(40000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(80000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(150000,42,0.8) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(160000,42,0.5) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(170000,42,0.4) should equal(None)
        threshold.sampleUpdateAndTest(180000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(190000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(200000,42,0.8) should equal(None)
        threshold.sampleUpdateAndTest(300000,42,0.8) should equal(Some(triggerEvent))
        threshold.sampleUpdateAndTest(360000,42,0.5) should equal(Some(releaseEvent))
        threshold.sampleUpdateAndTest(370000,42,0.4) should equal(None)

    }

}
