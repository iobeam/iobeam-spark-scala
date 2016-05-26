package com.iobeam.spark.streams.triggers

import org.apache.spark.streaming.Seconds
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.mock.MockitoSugar

/**
  * Created by ore on 15/05/16.
  */

class WindowTriggerTest extends FlatSpec with Matchers with MockitoSugar {

    val THRESHOLD = -5.0

    def triggerFunc(l: List[(Long, Any)]): Boolean = {
        val window = l.asInstanceOf[List[(Long, Double)]]
        (window.last._2 - window.head._2) <= THRESHOLD
    }

    "Window trigger" should "detect drop in window" in {
        val trigger = new WindowTrigger(Seconds(4), triggerFunc, "event")

        def s2us (t: Long) = t * 1000000

        // Fill up the window
        trigger.sampleUpdateAndTest(s2us(0), s2us(0), 100.0) should equal(None)
        trigger.window should equal(List((0,100.0)))

        trigger.sampleUpdateAndTest(s2us(2), s2us(2), 99.0) should equal(None)
        trigger.window should equal(List((0,100.0), (s2us(2), 99.0)))

        trigger.sampleUpdateAndTest(s2us(3), s2us(3), 98.0) should equal(None)
        trigger.window should equal(List((0,100.0), (s2us(2), 99.0), (s2us(3), 98.0)))

        trigger.sampleUpdateAndTest(s2us(4), s2us(4), 98.0) should equal(None)
        trigger.window should equal(List((s2us(2), 99.0), (s2us(3), 98.0), (s2us(4), 98.0)))

        // reordered input
        trigger.sampleUpdateAndTest(s2us(0), s2us(4), 98.0) should equal(None)
        trigger.window should equal(List((s2us(2), 99.0), (s2us(3), 98.0), (s2us(4), 98.0)))

        trigger.sampleUpdateAndTest(s2us(5), s2us(5), 98.0) should equal(None)
        trigger.window should equal(List((s2us(2), 99.0),(s2us(3), 98.0), (s2us(4), 98.0), (s2us(5), 98.0)))

        // Check that evaluation function is used correctly
        trigger.sampleUpdateAndTest(s2us(6), s2us(6), 90.0) should equal(Some("event"))
        trigger.window should equal(
                List((s2us(3), 98.0), (s2us(4), 98.0), (s2us(5), 98.0), (s2us(6), 90.0)))

        trigger.sampleUpdateAndTest(s2us(7), s2us(7), 80.0) should equal(None)
    }

}
