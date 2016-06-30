package com.iobeam.spark.streams.model.namespaces

/**
  * Holds namespace and field names
  */

object NamespaceField {
    val ALLOWED_STRINGS = "^[a-zA-Z0-9_-]+$"
}

case class NamespaceField(namespace: String, field: String) {

    def apply(namespace: String,
              field: String): NamespaceField = {

        if (!namespace.matches(NamespaceField.ALLOWED_STRINGS)) {
            throw new IllegalArgumentException(s"Bad namespace name: $namespace")
        }

        if (!field.matches(NamespaceField.ALLOWED_STRINGS)) {
            throw new IllegalArgumentException(s"Bad field name: $field")
        }

        new NamespaceField(namespace, field)
    }
}
