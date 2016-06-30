package com.iobeam.spark.streams.util

import java.nio.charset.StandardCharsets
import java.util.UUID

/**
  * Utility functions.
  */
object AppUtil extends Serializable {

    def createConfigUuid(projectId: Long, deviceId: String, seriesName: String): UUID = {
        UUID.nameUUIDFromBytes((projectId.toString + deviceId + seriesName)
            .getBytes(StandardCharsets.UTF_8))
    }

    def convertMapToJava(map: Map[String, Any]): java.util.Map[String, AnyRef] = {
        import scala.collection.JavaConverters._
        // scalastyle:ignore
        map.asInstanceOf[Map[String, AnyRef]].asJava
    }

    def convertMapToScala(map: java.util.Map[String, AnyRef]): Map[String, Any] = {
        import scala.collection.JavaConverters._
        // scalastyle:ignore
        map.asScala.toMap.asInstanceOf[Map[String, Any]]
    }
}
