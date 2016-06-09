package com.iobeam.spark.streams.transforms

/**
  * Contains the configuration for a field transform.
  */
abstract class FieldTransform extends Serializable {
    def getNewTransform: FieldTransformState
}
