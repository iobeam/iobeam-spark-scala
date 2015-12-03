package org.apache.spark

import com.iobeam.spark.streams.model.TimeRecord
import com.iobeam.spark.streams.testutils.SparkStreamingSpec
import com.iobeam.spark.streams.util.{BatchStateUtils, TestTimeRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.ClockWrapper
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}
import org.scalatest.{BeforeAndAfter, FlatSpec, GivenWhenThen, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Tests on utils
 */

object BatchStateUtilsTest {
    val BATCH_SIZE_MS = 1000
}

class BatchStateUtilsTest extends FlatSpec with SparkStreamingSpec with Matchers with
BeforeAndAfter with GivenWhenThen with Eventually {
    // scalastyle:off
    val batches = List(
        List(
            new TestTimeRecord(1, 1),
            new TestTimeRecord(2, 2), // Reordering
            new TestTimeRecord(0, 2),
            new TestTimeRecord(3, 1),
            new TestTimeRecord(4, 1) // Dup
        ),
        List(
            new TestTimeRecord(5, 1), // Dup
            new TestTimeRecord(6, 1), // Dup
            new TestTimeRecord(7, 42), // This should be kept
            new TestTimeRecord(8, 3),
            new TestTimeRecord(9, 4),
            new TestTimeRecord(10, 7)
        ),
        List(
            new TestTimeRecord(11, 7), // Dup
            new TestTimeRecord(12, 7) // Dup
        ),
        List(
            new TestTimeRecord(13, 7), // Dup
            new TestTimeRecord(14, 2)
        )
    )

    val correctOutput = List(
        List(
            new TestTimeRecord(0, 2),
            new TestTimeRecord(1, 1),
            new TestTimeRecord(2, 2),
            new TestTimeRecord(3, 1)
        ),
        List(
            new TestTimeRecord(7, 42),
            new TestTimeRecord(8, 3),
            new TestTimeRecord(9, 4),
            new TestTimeRecord(10, 7)
        ),
        List(),
        List(
            new TestTimeRecord(14, 2)
        )
    )
    // scalastyle:on

    // default timeout for eventually trait
    implicit override val patienceConfig =
        PatienceConfig(timeout = scaled(Span(5000, Millis))) // scalastyle:ignore

    "An output DStream" should "have adjacent duplicate values removed" in {

        Given("streaming context is initialized")
        val batchQueue = mutable.Queue[RDD[TimeRecord]]()
        val results = ListBuffer.empty[List[TimeRecord]]

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        val deviceDatasetConf = inputStream.map(a => ("TestDevice", a))

        val compressedStream = BatchStateUtils.removeDuplicateEventsFromStream(deviceDatasetConf,
            (a: TimeRecord, b: TimeRecord) =>
                a.asInstanceOf[TestTimeRecord].getValue != b.asInstanceOf[TestTimeRecord].getValue
                  , TimeRecord.TimeOrder)

        compressedStream.print()

        compressedStream.foreachRDD {
            rdd => results.append(rdd.map(a => a._2).collect().toList)
        }

        val clock = new ClockWrapper(ssc)

        ssc.start()

        for ((batch, i) <- batches.zipWithIndex) {
            batchQueue += ssc.sparkContext.makeRDD(batch)
            clock.advance(BatchStateUtilsTest.BATCH_SIZE_MS)

            eventually {
                results.length should equal(i + 1)
            }

            results.last should equal(correctOutput(i))
        }

        println(results)
        ssc.stop()
    }
}

