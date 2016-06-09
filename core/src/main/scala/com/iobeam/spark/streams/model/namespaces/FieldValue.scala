package com.iobeam.spark.streams.model.namespaces

/**
  * Labels used for queries.
  */

object FieldValue {
    def apply(labelName: String, labelValue: String) = new FieldValue(labelName, labelValue)

    def apply(labelName: String,
              labelValue: Double) = new FieldValue(labelName, labelValue: java.lang.Double)

    def apply(labelName: String,
              labelValue: Long) = new FieldValue(labelName, labelValue: java.lang.Long)

    def apply(labelName: String,
              labelValue: Boolean) = new FieldValue(labelName, labelValue: java.lang.Boolean)
}

case class FieldValue protected[namespaces](labelName: String, labelValue: AnyRef)
