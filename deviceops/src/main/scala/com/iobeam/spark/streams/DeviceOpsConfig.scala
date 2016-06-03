package com.iobeam.spark.streams

import com.iobeam.spark.streams.filters.{DeviceFilter, SeriesFilter, DeviceFilterConfiguration,
SeriesFilterConfiguration}
import com.iobeam.spark.streams.triggers.{DeviceTrigger, SeriesTrigger}

/**
  * Store of configured filters.
  */

class DeviceOpsConfigBuilder extends Serializable {
    val seriesFilters = scala.collection.mutable.Map[String, Seq[SeriesFilterConfiguration]]()
    val seriesTriggers = scala.collection.mutable.Map[String, Seq[SeriesTrigger]]()
    var deviceFilters = Seq[DeviceFilterConfiguration]()
    var deviceTriggers = Seq[DeviceTrigger]()

    def addSeriesFilter(seriesName: String,
                        outSeriesName: String,
                        filter: SeriesFilter): DeviceOpsConfigBuilder = {

        this.seriesFilters(seriesName) = (this.seriesFilters.getOrElse(seriesName, Seq())
            ++ Seq(new SeriesFilterConfiguration(outSeriesName, filter)))
        this
    }

    def addSeriesTrigger(seriesName: String,
                         seriesTrigger: SeriesTrigger): DeviceOpsConfigBuilder = {

        this.seriesTriggers(seriesName) = (this.seriesTriggers.getOrElse(seriesName, Seq())
            ++ Seq(seriesTrigger))
        this
    }

    def addDeviceFilter(outSeriesName: String,
                        deviceFilter: DeviceFilter): DeviceOpsConfigBuilder = {
        this.deviceFilters ++= Seq(new DeviceFilterConfiguration(outSeriesName, deviceFilter))
        this
    }

    def addDeviceTrigger(deviceTrigger: DeviceTrigger): DeviceOpsConfigBuilder = {
        this.deviceTriggers ++= Seq(deviceTrigger)
        this
    }

    def build: DeviceOpsConfig = {
        new DeviceOpsConfig(this.seriesFilters.toMap,
            this.seriesTriggers.toMap,
            this.deviceFilters,
            this.deviceTriggers)
    }
}

class DeviceOpsConfig(seriesFilters: Map[String, Seq[SeriesFilterConfiguration]],
                      seriesTriggers: Map[String, Seq[SeriesTrigger]],
                      deviceFilters: Seq[DeviceFilterConfiguration],
                      deviceTriggers: Seq[DeviceTrigger]) extends Serializable {

    def getSeriesFilters: Map[String, Seq[SeriesFilterConfiguration]] = seriesFilters

    def getDeviceTriggers: Seq[DeviceTrigger] = deviceTriggers

    def getDeviceFilters: Seq[DeviceFilterConfiguration] = deviceFilters

    def getSeriesTriggers: Map[String, Seq[SeriesTrigger]] = seriesTriggers

    /* creates a copy of the object with the same configuration but not necessarily the same
       internal state.
     */
    def create: DeviceOpsConfig = new DeviceOpsConfig(
        //map(identity) due to https://issues.scala-lang.org/browse/SI-7005
        seriesFilters.mapValues(sfSeq => sfSeq.map(sfc => sfc.copy())).map(identity),
        seriesTriggers.mapValues(stSeq => stSeq.map(st => st.create)).map(identity),
        deviceFilters.map(dfc => dfc.copy()),
        deviceTriggers.map(dt => dt.create)
    )

}
