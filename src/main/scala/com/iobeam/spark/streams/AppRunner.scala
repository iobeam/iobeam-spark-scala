package com.iobeam.spark.streams

import org.apache.logging.log4j
import org.apache.logging.log4j.LogManager

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.iobeam.spark.streams.model.TimeRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, ClockWrapper, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Runs spark apps on CSV files and verifies the results against expected output.
 */

object AppRunner {

    val BATCH_DURATION_MILLISECONDS = 1000
    val BATCH_DURATION = Milliseconds(BATCH_DURATION_MILLISECONDS)
    val BATCH_AWAIT_OUTPUT_SLEEP_MILLISECONDS = 100

    class SimpleCSVHeader(header:Array[String], firstRow: Array[String]) extends Serializable {
        val index = header.zipWithIndex.toMap
        val reverse = index.map(_.swap)

        val types = reverse.map{
            case (idx, value) =>
                val typed: TimeRecord.DataType = firstRow(idx) match {
                    case TimeRecord.DoubleType(v) => TimeRecord.DoubleType
                    case TimeRecord.BooleanType(v) => TimeRecord.BooleanType
                    case TimeRecord.StringType(v) => TimeRecord.StringType
                }
                idx -> typed
        }

        def apply(array:Array[String], key:String):String = array(index(key))

        def toDataSet(array:Array[String]): TimeRecord = {

            val full = array.zipWithIndex.map{
                case(value, idx) =>
                    val dataType = types(idx)
                    val converted = value match {
                        case dataType(v) => v
                    }
                reverse(idx) -> converted
            }.toMap
            val time = full("time").asInstanceOf[Double].toLong
            new TimeRecord(time, full-"time")
        }
    }

    def loadFile(sc: SparkContext, filename: String): RDD[TimeRecord] = {
        val csv = sc.textFile(filename) // original file
        val data = csv.map(line => line.split(",").map(elem => elem.trim)) //lines in rows
        val header = new AppRunner.SimpleCSVHeader(data.take(1)(0), data.take(2)(1)) // we build our header with the first line
        val rows = data.zipWithIndex().filter(_._2 > 0).map(_._1) //drop header
        rows.map(header.toDataSet)
    }
}

/**
  * Runs spark apps on CSV files and verifies the results against expected output.
  *
  * @param app iobeam spark app to run.
  * @param inputDir Directory with input files
  * @param outputDir Directory with expected output
  */
class AppRunner(app: SparkApp, inputDir: java.net.URI, outputDir: java.net.URI ) {

    private val LOGGER: log4j.Logger = LogManager.getLogger(this.getClass)

    def compare(expected: Array[Array[TimeRecord]], actual: Array[Array[TimeRecord]]): Unit = {
        if (expected.deep == actual.deep) {
            LOGGER.info("Results same as expected")
        } else {
            var runIndex = 0
            for( (expectedRun, actualRun) <- expected zip actual) {
                runIndex += 1
                if (expectedRun.sameElements(actualRun)){
                   LOGGER.info("Run $runIndex same")
                } else {
                    LOGGER.error("Run $runIndex is not same")
                }

            }
            LOGGER.error("Results not same actual length = $actual.length, expected length = $expected.length")
        }
    }

    def run(): (Array[Array[TimeRecord]], Array[Array[TimeRecord]]) = {
        val inputFiles = new java.io.File(inputDir)
          .listFiles.filter(_.getName.endsWith(".csv"))

        val batchQueue = mutable.Queue[RDD[(String, TimeRecord)]]()

        val conf = new SparkConf().setMaster("local[2]")
        conf.setAppName(app.name)

        conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, AppRunner.BATCH_DURATION)

        // Create the QueueInputDStream and use it do some processing
        val inputStream = ssc.queueStream(batchQueue)

        var results = ListBuffer.empty[Array[TimeRecord]]
        val outputStreams = app.processStream(inputStream)
        val firstDerived = outputStreams.getTimeSeries.head

        firstDerived.getDStream.map{ case (dev, ds: TimeRecord) => ds }.foreachRDD(
            (rdd, time) => {
                val res = rdd.collect()
                results += res.sortBy( ds => ds.time)
            }
        )

        val cw = new ClockWrapper(ssc)

        ssc.start()
        for (file <- inputFiles) {
            val startLength = results.length
            val ds = AppRunner.loadFile(sc, file.getAbsolutePath)
            val input = ds.map(item => ("TestDevice", item))
            batchQueue += input

            cw.advance(AppRunner.BATCH_DURATION_MILLISECONDS)

            var totalSleep = 0
            while(results.length == startLength && totalSleep < AppRunner.BATCH_DURATION_MILLISECONDS) {
                totalSleep += AppRunner.BATCH_AWAIT_OUTPUT_SLEEP_MILLISECONDS
                Thread.sleep(AppRunner.BATCH_AWAIT_OUTPUT_SLEEP_MILLISECONDS)
            }
        }

        val expected = for (file <- inputFiles) yield {
            AppRunner.loadFile(sc, outputDir + "/" + file.getName).collect()
        }
        ssc.stop()
        (expected, results.toArray)
    }
}
