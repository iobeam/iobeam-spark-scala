package com.iobeam.spark.streams.model

/**
  * A trigger event. Iobeam's trigger-execution system will perform
  * an action for each event created by the system. The action that
  * will be performed is configured through a REST API decoupled
  * from the spark app. Examples of actions include sending an email
  * or pushing data to an external system like MQTT. The actions
  * to be performed are determined by event names.
  *
  * @param name Name of triggerEvent. Determines the action to be performed.
  * @param data Data for the event.
  */
case class TriggerEvent(name: String, data: TimeRecord)

object TriggerEvent {
    def apply(name: String, time: Long, data: Map[String, Any]) =
        new TriggerEvent(name, new TimeRecord(time, data))
}
