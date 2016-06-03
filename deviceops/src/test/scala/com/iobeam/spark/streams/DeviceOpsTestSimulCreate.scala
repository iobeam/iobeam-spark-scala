package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.{TimeRecord, TriggerEvent}
import com.iobeam.spark.streams.triggers.ThresholdTrigger
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.ClockWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class DeviceOpsTestSimulCreate extends FlatSpec with Matchers with SparkStreamingSpec with GivenWhenThen with Eventually {

    val batches = List(
        List( //
            ("TestDev", new TestTimeRecord(10, 1.0)),
            ("TestDev", new TestTimeRecord(11, 100.0)) ,
            ("TestDev", new TestTimeRecord(12, 2.0)) ,
            ("TestDev1", new TestTimeRecord(13, 100.0)),
            ("TestDev2", new TestTimeRecord(13, 100.0)),
            ("TestDev3", new TestTimeRecord(13, 100.0)),
            ("TestDev4", new TestTimeRecord(13, 100.0)),
            ("TestDev5", new TestTimeRecord(13, 100.0)),
            ("TestDev6", new TestTimeRecord(13, 100.0)),
            ("TestDev7", new TestTimeRecord(13, 100.0)),
            ("TestDev8", new TestTimeRecord(13, 100.0)),
            ("TestDev9", new TestTimeRecord(13, 100.0)),
            ("TestDev10", new TestTimeRecord(13, 100.0)),
            ("TestDev11", new TestTimeRecord(13, 100.0)),
            ("TestDev12", new TestTimeRecord(13, 100.0)),
            ("TestDev22", new TestTimeRecord(13, 100.0)),
            ("TestDev23", new TestTimeRecord(13, 100.0)),
            ("TestDev24", new TestTimeRecord(13, 100.0)),
            ("TestDev25", new TestTimeRecord(13, 100.0)),
            ("TestDev26", new TestTimeRecord(13, 100.0)),
            ("TestDev27", new TestTimeRecord(13, 100.0)),
            ("TestDev28", new TestTimeRecord(13, 100.0)),
            ("TestDev29", new TestTimeRecord(13, 100.0)),
            ("TestDev30", new TestTimeRecord(13, 100.0)),
            ("TestDev31", new TestTimeRecord(13, 100.0))
        ),
        List( //  does not repeat across batches
            ("TestDev", new TestTimeRecord(13, 1.0)),
            ("TestDev", new TestTimeRecord(14, 2.0))
        ),
        List (
            ("TestDev1", new TestTimeRecord(24, 100.0)),
            ("TestDev2", new TestTimeRecord(24, 100.0)),
            ("TestDev3", new TestTimeRecord(24, 100.0)),
            ("TestDev4", new TestTimeRecord(24, 100.0)),
            ("TestDev5", new TestTimeRecord(24, 100.0)),
            ("TestDev6", new TestTimeRecord(24, 100.0)),
            ("TestDev7", new TestTimeRecord(24, 100.0)),
            ("TestDev8", new TestTimeRecord(24, 100.0)),
            ("TestDev9", new TestTimeRecord(24, 100.0)),
            ("TestDev10", new TestTimeRecord(24, 100.0)),
            ("TestDev11", new TestTimeRecord(24, 100.0)),
            ("TestDev12", new TestTimeRecord(24, 100.0)),
            ("TestDev22", new TestTimeRecord(24, 100.0)),
            ("TestDev23", new TestTimeRecord(24, 100.0)),
            ("TestDev24", new TestTimeRecord(24, 100.0)),
            ("TestDev25", new TestTimeRecord(24, 100.0)),
            ("TestDev26", new TestTimeRecord(24, 100.0)),
            ("TestDev27", new TestTimeRecord(24, 100.0)),
            ("TestDev28", new TestTimeRecord(24, 100.0)),
            ("TestDev29", new TestTimeRecord(24, 100.0)),
            ("TestDev30", new TestTimeRecord(24, 100.0)),
            ("TestDev31", new TestTimeRecord(24, 100.0))
        ),
        List (
            ("TestDev", new TestTimeRecord(25, 2.0))//quiet
        ),
        List (
            ("TestDev", new TestTimeRecord(26, 100.0)), //reset
            ("TestDev", new TestTimeRecord(27, 2.0))//fire
        )
    )

    val correctOutput = List(
        List( //0
            "lowBattery",
            "lowBattery"
        ),
        List ( //1
        ),List(//2
        ),List(//3
        ),List( //4
            "lowBattery"
        )
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "A DeviceOps DStream" should "process triggers correctly (checks the simultaneous device creation case)" in {

        val config = new DeviceOpsConfigBuilder()
            .addSeriesTrigger("value", new ThresholdTrigger(10.0, "lowBattery", 15.0))
            .build


        val batchQueue = mutable.Queue[RDD[(String, TimeRecord)]]()
        val results = ListBuffer.empty[List[(String, TriggerEvent)]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        // The deviceId is not used in this example
        val deviceTimeRecord = inputStream.map(a => (a._1, a._2))


        val (series, triggerStream) = DeviceOps.getDeviceOpsOutput(deviceTimeRecord, config)


        triggerStream.foreachRDD {
            rdd => results.append(rdd.collect().toList)
        }

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch)

            clock.advance(1000)
            eventually {
                results.length should equal(i+1)
            }

            results.last.length should equal(correctOutput(i).length)
        }

        ssc.stop()
    }
}

