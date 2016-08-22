package com.iobeam.spark.streams.transforms

/**
  * Holds a field transform and where its output should go.
  */
case class FieldTransformConfiguration(outputField: String, transform: FieldTransformState)
