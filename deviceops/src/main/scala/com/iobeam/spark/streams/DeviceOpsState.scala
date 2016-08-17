package com.iobeam.spark.streams

import com.iobeam.spark.streams.model.namespaces.NamespaceField
import com.iobeam.spark.streams.transforms._

/**
  * Store of configured filters.
  */
object DeviceOpsConfig {
    val DEFAULT_READ_NAMESPACE = "input"
    val DEFAULT_WRITE_NAMESPACE = "device_ops"
}

class DeviceOpsConfig extends Serializable {

    val fieldsTransforms = scala.collection.mutable.Map[NamespaceField,
        Seq[(NamespaceField, FieldTransform)]]()
    val namespaceTransforms = scala.collection.mutable.Map[String,
        Seq[(NamespaceField, NamespaceTransform)]]()

    var writeNamespace = DeviceOpsConfig.DEFAULT_WRITE_NAMESPACE

    def addFieldTransform(readNamespace: String,
                          readFieldName: String,
                          writeNamespace: String,
                          writeFieldName: String,
                          transform: FieldTransform): DeviceOpsConfig = {

        this.fieldsTransforms(NamespaceField(readNamespace, readFieldName)) = (this
            .fieldsTransforms.getOrElse(
            NamespaceField(readNamespace, readFieldName), Seq())
            ++ Seq((NamespaceField(writeNamespace, writeFieldName), transform)))
        this
    }

    def addFieldTransform(readFieldName: String,
                          writeFieldName: String,
                          transform: FieldTransform): DeviceOpsConfig = {

        addFieldTransform(DeviceOpsConfig.DEFAULT_READ_NAMESPACE,
            readFieldName,
            DeviceOpsConfig.DEFAULT_WRITE_NAMESPACE,
            writeFieldName,
            transform
        )
    }

    def addNamespaceTransform(readNamespace: String,
                              writeNamespace: String,
                              writeField: String,
                              namespaceTransform: NamespaceTransform): DeviceOpsConfig = {
        this.namespaceTransforms(readNamespace) = (this.namespaceTransforms.getOrElse
        (readNamespace, Seq())
            ++ Seq((NamespaceField(writeNamespace, writeField), namespaceTransform)))

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
            this.namespaceTransforms
                .mapValues(nsConf => nsConf.map(a => (a._1, a._2.getNewTransform)))

        new DeviceOpsState(
            fieldTransformsState.toMap,
            namespaceTransformsState.toMap)
    }
}

class DeviceOpsState(fieldTransforms: Map[NamespaceField, Seq[FieldTransformConfiguration]],
                     namespaceTransforms: Map[String, Seq[(NamespaceField,
                         NamespaceTransformState)]])
    extends Serializable {

    def getFieldsTransforms: Map[NamespaceField, Seq[FieldTransformConfiguration]] =
        fieldTransforms

    def getNamespaceTransforms: Map[String, Seq[(NamespaceField, NamespaceTransformState)]] =
        namespaceTransforms

}
