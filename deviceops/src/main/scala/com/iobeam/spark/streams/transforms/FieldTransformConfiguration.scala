package com.iobeam.spark.streams.transforms

import com.iobeam.spark.streams.model.namespaces.NamespaceField

/**
  * Holds a field transform and where its output should go.
  */
case class FieldTransformConfiguration(outputSeries: NamespaceField, transform: FieldTransformState)
