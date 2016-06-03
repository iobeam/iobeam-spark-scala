package com.iobeam.spark.streams

import com.iobeam.spark.streams.filters.DerivativeFilter
import com.iobeam.spark.streams.model.{TriggerEvent, TimeRecord}
import com.iobeam.spark.streams.triggers.{DeviceTimeoutTrigger, ThresholdTrigger}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, ClockWrapper, Seconds}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class DeviceOpsTest extends FlatSpec with Matchers with SparkStreamingSpec with GivenWhenThen with Eventually {
    conf.setMaster("local")//test case with no partition for maximum ability to clash

    val batches = List(
        List( //
            ("TestDev", new TestTimeRecord(10, 1.0)),
            ("TestDev", new TestTimeRecord(11, 100.0)) ,
            ("TestDev", new TestTimeRecord(12, 2.0))
        ),
        List( //  does not repeat across batches
            ("TestDev", new TestTimeRecord(13, 1.0)),
            ("TestDev", new TestTimeRecord(14, 2.0))
        ),
        List( //  does not repeat within batches
            ("TestDev", new TestTimeRecord(15, 100.0)), //reset
            ("TestDev", new TestTimeRecord(16, 2.0)), //fire
            ("TestDev", new TestTimeRecord(17, 2.0)) //quiet
        ),
        List( // does not repeat; devices don't interfere
            ("TestDev", new TestTimeRecord(18, 100.0)), //reset
            ("TestDev", new TestTimeRecord(19, 1.0)), //fire
            ("TestDev1", new TestTimeRecord(20, 101.0)),  //does not reset
            ("TestDev", new TestTimeRecord(21, 2.0)) //quiet
        ),
        List (
            ("TestDev", new TestTimeRecord(22, 100.0)), //reset
            ("TestDev", new TestTimeRecord(23, 2.0))//fire
        ),
        List (
            ("TestDev1", new TestTimeRecord(24, 101.0))// should not reset, other device
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
        ),
        List ( //2
            "lowBattery"
        ),
        List( //3
            "lowBattery"
        ),
        List( //4
            "lowBattery"
        ),List(//5

        ),List(//6

        ),List( //7
            "lowBattery"
        )
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "A DeviceOp DStream" should "process triggers correctly" in {

        Given("streaming context is initialized")

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


       //val triggerStream = outputStreams.getTriggerSeriesDStreams.head



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

