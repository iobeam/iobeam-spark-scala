package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.testutils.TestFilter
import com.iobeam.spark.streams.transforms.ThresholdTrigger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.ClockWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class DeviceOpsTest extends FlatSpec with Matchers with SparkStreamingSpec with GivenWhenThen
    with Eventually {
    conf.setMaster("local")
    //test case with no partition for maximum ability to clash

    val batches = List(
        List(//
            ("TestDev", new TestTimeRecord(10,"TestDev", 1.0)),
            ("TestDev", new TestTimeRecord(11,"TestDev", 100.0)),
            ("TestDev", new TestTimeRecord(12,"TestDev", 2.0))
        ),
        List(//  does not repeat across batches
            ("TestDev", new TestTimeRecord(13,"TestDev", 1.0)),
            ("TestDev", new TestTimeRecord(14,"TestDev", 2.0))
        ),
        List(//  does not repeat within batches
            ("TestDev", new TestTimeRecord(15,"TestDev", 100.0)), //reset
            ("TestDev", new TestTimeRecord(16,"TestDev", 2.0)), //fire
            ("TestDev", new TestTimeRecord(17,"TestDev", 2.0)) //quiet
        ),
        List(// does not repeat; devices don't interfere
            ("TestDev", new TestTimeRecord(18,"TestDev", 100.0)), //reset
            ("TestDev", new TestTimeRecord(19,"TestDev", 1.0)), //fire
            ("TestDev1", new TestTimeRecord(20,"TestDev1", 101.0)), //does not reset
            ("TestDev", new TestTimeRecord(21,"TestDev", 2.0)) //quiet
        ),
        List(
            ("TestDev", new TestTimeRecord(22,"TestDev", 100.0)), //reset
            ("TestDev", new TestTimeRecord(23,"TestDev", 2.0)) //fire
        ),
        List(
            ("TestDev1", new TestTimeRecord(24,"TestDev1", 101.0)) // should not reset, other device
        ),
        List(
            ("TestDev", new TestTimeRecord(25,"TestDev", 2.0)) //quiet
        ),
        List(
            ("TestDev", new TestTimeRecord(26,"TestDev",100.0)), //reset
            ("TestDev", new TestTimeRecord(27,"TestDev", 2.0)) //fire
        )
    )

    val correctTriggerOutput = List(
        List(//0
            "lowBattery",
            "lowBattery"
        ),
        List(//1
        ),
        List(//2
            "lowBattery"
        ),
        List(//3
            "lowBattery"
        ),
        List(//4
            "lowBattery"
        ), List(//5

        ), List(//6

        ), List(//7
            "lowBattery"
        )
    )

    val correctSeriesOutput = List(
        List(//
            ("TestDev", (10,"TestDev", 0.0)),
            ("TestDev", (11,"TestDev", 1.0)),
            ("TestDev", (12, "TestDev",100.0))
        ),
        List(//  does not repeat across batches
            ("TestDev", (13,"TestDev", 2.0)),
            ("TestDev", (14,"TestDev", 1.0))
        ),
        List(//  does not repeat within batches
            ("TestDev", (15,"TestDev", 2.0)),
            ("TestDev", (16,"TestDev", 100.0)),
            ("TestDev", (17, "TestDev",2.0))
        ),
        List(// devices don't interfere
            ("TestDev", (18,"TestDev", 2.0)),
            ("TestDev", (19, "TestDev",100.0)),
            ("TestDev", (21,"TestDev", 1.0))
        ),
        List(
            ("TestDev", (22,"TestDev", 2.0)),
            ("TestDev", (23,"TestDev", 100.0))
        ),
        List(),
        List(
            ("TestDev", (25,"TestDev", 2.0))
        ),
        List(
            ("TestDev", (26, "TestDev",2.0)),
            ("TestDev", (27, "TestDev",100.0))
        )
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "A DeviceOp DStream" should "process triggers and filters correctly" in {

        Given("streaming context is initialized")

        val config = new DeviceOpsConfig()
            .addFieldTransform("value", "seriesOut", new TestFilter)
            .addFieldTransform("value", "triggerOut",
                new ThresholdTrigger(10.0, "lowBattery", 15.0))

        val batchQueue = mutable.Queue[RDD[(String, TimeRecord)]]()
        val seriesResults = ListBuffer.empty[List[TimeRecord]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        // The deviceId is not used in this example.filter(t => !t.has("lowBattery"))
        val deviceTimeRecord = inputStream.map(a => (a._1, a._2))

        val seriesStream = DeviceOps.getDeviceOpsOutput(deviceTimeRecord.map(a => a._2), "device_id", config)

        seriesStream.foreachRDD(
            rdd => seriesResults.append(rdd.collect.toList)
        )

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch)

            clock.advance(1000)
            eventually {
                seriesResults.length should equal(i + 1)
            }

            val lowBatt = seriesResults.last.filter(t => t.has("triggerOut"))
            val outSeries = seriesResults.last.filter(t => t.has("seriesOut"))

            lowBatt.length should equal(correctTriggerOutput(i).length)
            seriesResults.last
                // Drop the trigger series output
                .filter(t => !t.has("triggerOut"))
                // Drop potentially interfering device
                .filter(t => t.getString("device_id").get == "TestDev")
                // transform to (deviceId, (time, value))
                .map(a => (a.getString("device_id").get,
                (a.time, a.getDouble("seriesOut").get)))
                .sortBy(a => (a._1, a._2._1)) should
                equal(correctSeriesOutput(i).map(a => (a._1, (a._2._1, a._2._3))))

        }

        ssc.stop()
    }
}

