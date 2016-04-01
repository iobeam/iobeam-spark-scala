package com.iobeam.spark.streams.filters

import com.iobeam.spark.streams.model.TimeRecord

import scala.collection.mutable.ListBuffer

/**
  * Filters
  */

object SeriesFilter {

    def updateSeriesFilters(filters: Map[String, Seq[SeriesFilterConfiguration]],
                            timeRecord: TimeRecord): Seq[TimeRecord] = {

        val listBuilder = new ListBuffer[TimeRecord]

        for ((name, reading) <- timeRecord.getData) {

            // Check all triggers configured for the series
            for (filterConf <- filters.getOrElse(name, Seq())) {
                val out = filterConf.filter.update(timeRecord.time, reading)
                listBuilder.append(new TimeRecord(timeRecord.time, Map(filterConf.outputSeries ->
                    out)))
            }
        }
        listBuilder.toSeq
    }
}

abstract class SeriesFilter extends Serializable {

    def update(t: Long, newSample: Any): Any

    def getValue: Any
}
