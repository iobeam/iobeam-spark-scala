package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.transforms.{NamespaceTransform, NamespaceTransformState}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.ClockWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TestNamespaceTranform extends NamespaceTransform {

    class TestNamespaceTransformState extends NamespaceTransformState {
        /**
          * Called on each sample in series.3
          *
          * @param record time record
          * @return Option[triggerString] id trigger
          */
        override def recordUpdateAndTest(record: TimeRecord,
                                         batchTimeUs: Long): Option[Any] = None

        /**
          * Called when batch is done, used to update periodic state.
          * May return trigger.
          *
          * @param timeUs Batch time
          * @return Option[triggeString]
          */
        override def batchDoneUpdateAndTest(timeUs: Long): Option[Any] = Some("Event")
    }

    override def getNewTransform: NamespaceTransformState = new TestNamespaceTransformState
}

class NamespaceTransformTest extends FlatSpec with Matchers with SparkStreamingSpec with
    GivenWhenThen
    with Eventually {
    conf.setMaster("local")
    //test case with no partition for maximum ability to clash

    val batches = List(
        List(//
            ("TestDev", new TestTimeRecord(10, "TestDev", 1.0)),
            ("TestDev", new TestTimeRecord(11, "TestDev", 100.0)),
            ("TestDev", new TestTimeRecord(12, "TestDev", 2.0))
        ),
        List()
    )

    val correctSeriesOutput = List(
        List(new TimeRecord(1000000, Map("device_id" -> "TestDev", "output" -> "Event"))),
        List(new TimeRecord(1000000, Map("device_id" -> "TestDev", "output" -> "Event")))
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "A DeviceOp DStream" should "process triggers and filters correctly" in {

        Given("streaming context is initialized")

        val config = new DeviceOpsConfig()
            .addNamespaceTransform("output", new TestNamespaceTranform)

        val batchQueue = mutable.Queue[RDD[(String, TimeRecord)]]()
        val seriesResults = ListBuffer.empty[List[TimeRecord]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        // The deviceId is not used in this example.filter(t => !t.has("lowBattery"))
        val deviceTimeRecord = inputStream.map(a => a._2)

        val seriesStream = DeviceOps.getDeviceOpsOutput(deviceTimeRecord, "device_id", config)

        seriesStream.foreachRDD(
            rdd => seriesResults.append(rdd.collect.toList)
        )

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch)

            clock.advance(5000)
            eventually {
                seriesResults.length should equal(i + 1)
            }

            seriesResults.last should equal(correctSeriesOutput(i))

        }

        ssc.stop()
    }
}

