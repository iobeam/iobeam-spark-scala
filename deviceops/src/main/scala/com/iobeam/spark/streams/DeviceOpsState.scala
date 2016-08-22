package com.iobeam.spark.streams

import com.iobeam.spark.streams.transforms._

/**
  * Store of configured filters.
  */
object DeviceOpsConfig {
    val DEFAULT_READ_NAMESPACE = "input"
    val DEFAULT_WRITE_NAMESPACE = "device_ops"
}

class DeviceOpsConfig extends Serializable {

    val fieldsTransforms = scala.collection.mutable.Map[String,
        Seq[(String, FieldTransform)]]()
    var namespaceTransforms = scala.collection.mutable.Seq[(String, NamespaceTransform)]()

    var writeNamespace = DeviceOpsConfig.DEFAULT_WRITE_NAMESPACE

    def addFieldTransform(readFieldName: String,
                          writeFieldName: String,
                          transform: FieldTransform): DeviceOpsConfig = {

        this.fieldsTransforms(readFieldName) = (this
            .fieldsTransforms.getOrElse(readFieldName, Seq())
            ++ Seq((writeFieldName, transform)))
        this
    }

    def addNamespaceTransform(writeField: String,
                              namespaceTransform: NamespaceTransform): DeviceOpsConfig = {
        this.namespaceTransforms = this.namespaceTransforms ++ Seq((writeField, namespaceTransform))

        this
    }

    def setWriteNamespace(namespace: String) = {
        this.writeNamespace = namespace
    }

    def getWriteNamespace = this.writeNamespace

    def build: DeviceOpsState = {

        val fieldTransformsState = this.fieldsTransforms.mapValues(
            a => a.map(ft => new FieldTransformConfiguration(ft._1, ft._2.getNewTransform)))

        val namespaceTransformsState =
            this.namespaceTransforms.map(a => (a._1, a._2.getNewTransform))

        new DeviceOpsState(
            fieldTransformsState.toMap,
            namespaceTransformsState)
    }
}

class DeviceOpsState(fieldTransforms: Map[String, Seq[FieldTransformConfiguration]],
                     namespaceTransforms: Seq[(String,
                         NamespaceTransformState)])
    extends Serializable {

    def getFieldsTransforms: Map[String, Seq[FieldTransformConfiguration]] =
        fieldTransforms

    def getNamespaceTransforms: Seq[(String, NamespaceTransformState)] =
        namespaceTransforms

}
