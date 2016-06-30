package com.iobeam.spark.streams.transforms

/**
  * Transform that use more than one field from a namespace
  */
abstract class NamespaceTransform extends Serializable {
    def getNewTransform: NamespaceTransformState
}
