package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.transforms.ThresholdTrigger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.ClockWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class DeviceOpsTestSimulCreate extends FlatSpec with Matchers with SparkStreamingSpec with
    GivenWhenThen with Eventually {

    val batches = List(
        List(//
            new TestTimeRecord(10, "TestDev", 1.0),
            new TestTimeRecord(11, "TestDev",  100.0),
            new TestTimeRecord(12, "TestDev", 2.0),
            new TestTimeRecord(13, "TestDev1",  100.0),
            new TestTimeRecord(13, "TestDev2", 100.0),
            new TestTimeRecord(13, "TestDev3", 100.0),
            new TestTimeRecord(13, "TestDev4", 100.0),
            new TestTimeRecord(13, "TestDev5", 100.0),
            new TestTimeRecord(13, "TestDev6", 100.0),
            new TestTimeRecord(13, "TestDev7",  100.0),
            new TestTimeRecord(13, "TestDev8", 100.0),
            new TestTimeRecord(13, "TestDev9", 100.0),
            new TestTimeRecord(13, "TestDev10", 100.0),
            new TestTimeRecord(13, "TestDev11",  100.0),
            new TestTimeRecord(13, "TestDev12",  100.0),
            new TestTimeRecord(13, "TestDev22", 100.0),
            new TestTimeRecord(13, "TestDev23",  100.0),
            new TestTimeRecord(13, "TestDev24",  100.0),
            new TestTimeRecord(13, "TestDev25",  100.0),
            new TestTimeRecord(13, "TestDev26", 100.0),
            new TestTimeRecord(13, "TestDev27", 100.0),
            new TestTimeRecord(13, "TestDev28", 100.0),
            new TestTimeRecord(13, "TestDev29", 100.0),
            new TestTimeRecord(13, "TestDev30",100.0),
            new TestTimeRecord(13, "TestDev31",  100.0)
        ),
        List(//  does not repeat across batches
            new TestTimeRecord(13, "TestDev",  1.0),
            new TestTimeRecord(14, "TestDev",  2.0)
        ),
        List(
            new TestTimeRecord(24, "TestDev1", 100.0),
            new TestTimeRecord(24, "TestDev2",  100.0),
            new TestTimeRecord(24, "TestDev3", 100.0),
            new TestTimeRecord(24, "TestDev4", 100.0),
            new TestTimeRecord(24, "TestDev5", 100.0),
            new TestTimeRecord(24, "TestDev6", 100.0),
            new TestTimeRecord(24, "TestDev7", 100.0),
            new TestTimeRecord(24, "TestDev8", 100.0),
            new TestTimeRecord(24, "TestDev9", 100.0),
            new TestTimeRecord(24, "TestDev10", 100.0),
            new TestTimeRecord(24, "TestDev11", 100.0),
            new TestTimeRecord(24, "TestDev12", 100.0),
            new TestTimeRecord(24, "TestDev22", 100.0),
            new TestTimeRecord(24, "TestDev23", 100.0),
            new TestTimeRecord(24, "TestDev24", 100.0),
            new TestTimeRecord(24, "TestDev25", 100.0),
            new TestTimeRecord(24, "TestDev26", 100.0),
            new TestTimeRecord(24, "TestDev27", 100.0),
            new TestTimeRecord(24, "TestDev28", 100.0),
            new TestTimeRecord(24, "TestDev29", 100.0),
            new TestTimeRecord(24, "TestDev30", 100.0),
            new TestTimeRecord(24, "TestDev31", 100.0)
        ),
        List(
            new TestTimeRecord(25, "TestDev", 2.0) //quiet
        ),
        List(
            new TestTimeRecord(26, "TestDev", 100.0), //reset
            new TestTimeRecord(27, "TestDev", 2.0) //fire
        )
    )

    val correctOutput = List(
        List(//0
            "lowBattery",
            "lowBattery"
        ),
        List(//1
        ), List(//2
        ), List(//3
        ), List(//4
            "lowBattery"
        )
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "A DeviceOps DStream" should "process triggers correctly (checks the simultaneous device " +
        "creation case)" in {

        val config = new DeviceOpsConfig()
            .addFieldTransform("value", "output", new ThresholdTrigger(10.0, "lowBattery", 15.0))

        val batchQueue = mutable.Queue[RDD[(String, TimeRecord)]]()
        val results = ListBuffer.empty[List[TimeRecord]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        // The deviceId is not used in this example
        val timeRecordStrem = inputStream.map(a => a._2)


        val triggerStream = DeviceOps.getDeviceOpsOutput(timeRecordStrem, "device_id", config)

        triggerStream.foreachRDD {
            rdd => results.append(rdd.collect().toList)
        }

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch.map({
                case(tr: TimeRecord) => (tr.device_id, tr)
            }))

            clock.advance(1000)
            eventually {
                results.length should equal(i + 1)
            }

            results.last.length should equal(correctOutput(i).length)
        }

        ssc.stop()
    }
}

