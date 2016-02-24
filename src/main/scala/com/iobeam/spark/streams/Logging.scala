package com.iobeam.spark.streams

import java.util.concurrent.atomic.AtomicReference

import com.iobeam.spark.streams.Logging.Initializer
import org.apache.logging.log4j.{LogManager, Logger}

/**
  * Adds logging capabilities to a class.
  */
trait Logging {

    private val initializer = new AtomicReference[Initializer](new Initializer {
        override def createLogger(clazz: Class[_]): Logger = LogManager.getLogger(clazz)
    })

    @transient private lazy val logger = initializer.get.createLogger(getClass)

    def setLoggerInitializer(initializer: Initializer): Unit = {
        this.initializer.set(initializer)
    }

    def getLogger: Logger = {
        logger
    }

    protected def logWarn(msg: => String) {
        getLogger.warn(msg)
    }

    protected def logDebug(msg: => String) {
        getLogger.debug(msg)
    }

    protected def logInfo(msg: => String) {
        getLogger.info(msg)
    }

    protected def logTrace(msg: => String) {
        getLogger.trace(msg)
    }

    protected def logError(msg: => String) {
        getLogger.error(msg)
    }

    protected def logFatal(msg: => String) {
        getLogger.fatal(msg)
    }

    protected def logWarn(msg: => String, t: Throwable) {
        getLogger.warn(msg, t)
    }

    protected def logDebug(msg: => String, t: Throwable) {
        getLogger.debug(msg, t)
    }

    protected def logInfo(msg: => String, t: Throwable) {
        getLogger.info(msg, t)
    }

    protected def logTrace(msg: => String, t: Throwable) {
        getLogger.trace(msg, t)
    }

    protected def logError(msg: => String, t: Throwable) {
        getLogger.error(msg, t)
    }

    protected def logFatal(msg: => String, t: Throwable) {
        getLogger.fatal(msg, t)
    }

    protected def catching(t: Throwable) {
        getLogger.catching(t)
    }
}

object Logging {

    abstract class Initializer extends Serializable {
        def createLogger(clazz: Class[_]): Logger
    }

}
