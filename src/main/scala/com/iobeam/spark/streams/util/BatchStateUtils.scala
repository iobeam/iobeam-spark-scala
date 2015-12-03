package com.iobeam.spark.streams.util

import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * Utilities to handle states between micro batches.
 *
 */

object BatchStateUtils extends Serializable {
   /**
     *
     * Returns a DStream containing only state changes within the input DStream.
     * State change is defined by isStateChange.
     *
     * Example:
     * {{{
     *
     * val stateStream = BatchStateUtils
     *                  .removeDuplicateEventsFromStream(rawStream,
     *                                                   (a: TimeRecord, b: TimeRecord) =>
     *                                                       a.getBoolean("state") != b.getBoolean("state"),
     *                                                  TimeRecord.TimeOrder)
     *
     *
     * }}}
     *
     * @param eventStream stream of readings with a partition key.
     * @param isStateChange function that checks whether adjacent readings show a state change
     * @param ord: ordering on the readings to be performed within a partition
     *
     * @return stream of readings that correspond state changes
     */

    def removeDuplicateEventsFromStream[T: ClassTag](eventStream: DStream[(String, T)],
                            isStateChange: (T, T) => Boolean, ord: Ordering[T]):
    DStream[(String, T)] = {

        var stateMap = Map[String,T]()

        def removeFirstEventIfSameAsLastBatch(t: (String, List[T])):
        (String, List[T]) = {

            val (key, events: List[T]) = t
            val sortedEvents = events.sorted(ord)

            val filtered = if (stateMap.contains(key)) {
                val previousEvent = stateMap(key)
                sortedEvents match {
                    case first :: rest =>
                        if (!isStateChange(previousEvent, first))
                            rest
                        else first :: rest
                    case List() =>
                        sortedEvents
                }

            } else sortedEvents

            (key, filtered)
        }

        def removeAdjacentDuplicates[A](ls: List[T]):
        List[T] = {
            ls.sorted(ord).reverse.foldRight(List[T]()) { (h, r) =>
                if (r.isEmpty || isStateChange(r.head, h))
                    h :: r
                else r
            }
        }

        //TODO: remove groupByKey --- not efficient
        val sortedBatchEvents = eventStream.groupByKey().mapValues(
            l => l.toList.sorted(ord))

        val duplicatesRemoved = sortedBatchEvents.map(a => {
            removeFirstEventIfSameAsLastBatch((a._1, removeAdjacentDuplicates(a._2)))
        })

        val lastEvents = duplicatesRemoved.flatMapValues(a =>
            if (a.nonEmpty)
                List(a.last)
            else
                List()
        )

        // Persist so we do not rerun removeAdjacentDuplicates etc.
        duplicatesRemoved.persist()

        lastEvents.foreachRDD(rdd => {
            stateMap = stateMap ++ rdd.collect().toMap
        })

        duplicatesRemoved.flatMapValues(a => a)
    }
}
