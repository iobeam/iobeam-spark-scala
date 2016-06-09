package spark.streams.spark

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.{DeJitterStream, TestTimeRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{ClockWrapper, Seconds}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import spark.streams.testutils.SparkStreamingSpec

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class PlayoutBufferTest extends FlatSpec with Matchers with SparkStreamingSpec with GivenWhenThen
    with Eventually {

    val batches = List(
        List(// 0-1000
            new TestTimeRecord(10, 1),
            new TestTimeRecord(20, 2)
        ),
        List(// 1000 - 2000
            new TestTimeRecord(1010, 3),
            new TestTimeRecord(1020, 4),
            new TestTimeRecord(30, 5) // out of order
        ),
        List(// 2000 - 3000
            new TestTimeRecord(2010, 6),
            new TestTimeRecord(25, 7) // to old
        ),
        List(// 3000 - 4000
            new TestTimeRecord(2020, 8),
            new TestTimeRecord(2030, 9)
        ),
        List()
    )

    val correctOutput = List(
        List(),
        List(
            (10, 1),
            (20, 2),
            (30, 5)
        ),
        List(
            (1010, 3),
            (1020, 4)
        ),
        List(
            (2010, 6),
            (2020, 8),
            (2030, 9)
        ),
        List()
    )

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(15000, Millis)))

    "An output DStream" should "have the values increased by one" in {

        Given("streaming context is initialized")
        val batchQueue = mutable.Queue[RDD[TimeRecord]]()
        val results = ListBuffer.empty[List[(Long, Double)]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        // The deviceId is not used in this example
        val deviceTimeRecord = inputStream.map(a => ("TestDevice", a))

        val outputStream = DeJitterStream.getTimeSlot(deviceTimeRecord, Seconds(2), Seconds(1))

        outputStream.foreachRDD {
            rdd => results.append(
                rdd.map(a => (a._2.time, a._2.requireDouble("value")))
                    .collect().toList)
        }

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch)

            clock.advance(1000)
            eventually {
                results.length should equal(i + 1)
            }

            results.last should equal(correctOutput(i))
        }

        ssc.stop()
    }
}

