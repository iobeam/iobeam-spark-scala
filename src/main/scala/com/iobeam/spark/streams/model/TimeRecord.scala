package com.iobeam.spark.streams.model

/**
 * A Record represents a collection of data for a specific time-point.
 */
object TimeRecord {
    type Key = String

    /**
      * Defines how records convert values to Strings, Doubles, and Booleans.
      */
    sealed abstract class DataType extends Serializable {
        def unapply(x: Any): Option[Any]
    }

    object StringType extends DataType {
        def apply(s: String): String = s

        def unapply(x: Any): Option[String] = x match {
            case s: String => Some(s)
            case _ => None
        }
    }

    object BooleanType extends DataType {
        def apply(b: Boolean): Boolean = b

        def unapply(x: Any): Option[Boolean] = x match {
            case s: String => if (s == "true") Some(true) else if (s == "false") Some(false) else None
            case b: Boolean => Some(b)
            case _ => None
        }
    }

    object DoubleType extends DataType {
        def apply(b: Double): Double = b

        def unapply(x: Any): Option[Double] = x match {
            case s: String => {
                import scala.util.control.Exception.allCatch
                (allCatch opt s.toDouble)
            }
            case b: Double => Some(b)
            case _ => None
        }
    }

    /**
      * Defines an Ordering on Records by time.
      */
    object TimeOrder extends Ordering[TimeRecord] {
        def compare(one: TimeRecord, that: TimeRecord): Int =  (one.time - that.time).toInt
    }
}

/**
  *
  * A TimeRecord represents a collection of data for one time-point. The data
  * is stored as a key-value map.
  *
  * For data imported to Iobeam, each TimeRecord represents a collection of
  * measurements taken at a single time-point. Thus the data is a map of
  * seriesName (as the key) to measurement value. Data imported via the table format has a guarantee
  * that each row will be represented by a single record which contains
  * all of the series in the row. For the
  * series-centric input format, each record will contain a single
  * seriesName and value (there will be no join on time across series).
  *
  *
  * @param time The time at which the measurement was taken
  * @param data A collection of measurements
  */
case class TimeRecord(val time: Long, val data: Map[TimeRecord.Key, Any] = Map())  extends Serializable {

    /**
      * Returns a map of all key-value pairs stored in the record
      * @return
      */
    def getData: Map[TimeRecord.Key, Any] = data

    /**
      * Returns a new record with additional data.
      * @param other data to add
      * @return
      */
    def ++(other: Map[TimeRecord.Key, Any]): TimeRecord = new TimeRecord(time, data ++ other)

    /**
      * Returns whether or not all the keys are present.
      * @param keys
      * @return
      */
    def has(keys: TimeRecord.Key*): Boolean = keys.forall(key => data.contains(key))

    /**
      * Return the value (as a String) for a key,
      * throwing an error if the key is missing or cannot be converted to a String.
      * @param key
      * @return
      */
    def requireString(key: TimeRecord.Key): String  = (getString(key): @unchecked) match {case Some(v) => v}

    /**
      * Return the value (as a Double) for a key, throwing an error if the key
      * is missing or cannot be converted to a Double.
      * @param key
      * @return
      */
    def requireDouble(key: TimeRecord.Key): Double  = (getDouble(key): @unchecked) match {case Some(v) => v}

    /**
      * Return the value (as a Boolean) for a key, throwing an error if the key
      * is missing or cannot be converted to a Double.
      * @param key
      * @return
      */
    def requireBoolean(key: TimeRecord.Key): Boolean = (getBoolean(key): @unchecked) match {case Some(v) => v}


    /**
      * Returns an option for a String value corresponding to a key.
      * The option is None if the key does not exist or the value
      * cannot be converted to a String.
      * @param key
      * @return
      */
    def getString(key: TimeRecord.Key): Option[String]  = {
        val value = data.get(key)
        value match {
            case Some(v) => v match {
                case TimeRecord.StringType(str) => Some(str)
                case _ => None
            }
            case None => None
        }
    }

    /**
      * Returns an option for a Double value corresponding to a key.
      * The option is None if the key does not exist or the value
      * cannot be converted to a Double.
      * @param key
      * @return
      */
    def getDouble(key: TimeRecord.Key): Option[Double]  = {
        val value = data.get(key)
        value match {
            case Some(v) => v match {
                case TimeRecord.DoubleType(str) => Some(str)
                case _ => None
            }
            case None => None
        }
    }

    /**
      * Returns an option for a Boolean value corresponding to a key.
      * The option is None if the key does not exist or the value
      * cannot be converted to a Boolean.
      * @param key
      * @return
      */
    def getBoolean(key: TimeRecord.Key): Option[Boolean]  = {
        val value = data.get(key)
        value match {
            case Some(v) => v match {
                case TimeRecord.BooleanType(str) => Some(str)
                case _ => None
            }
            case None => None
        }
    }

}
