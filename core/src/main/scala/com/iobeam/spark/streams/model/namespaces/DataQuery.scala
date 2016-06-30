package com.iobeam.spark.streams.model.namespaces

import scala.collection.mutable.ListBuffer

/**
  * Contains a query for a namespace.
  *
  * if the set of includes and excludes overlap, excludes override.
  */
object DataQuery {
    /**
      * Build a namespace query.
      */
    class DataQueryBuilder private[namespaces](namespaceName: String) {
        private var include = ListBuffer[FieldValue]()
        private var exclude = ListBuffer[FieldValue]()

        private[namespaces] def addExclude(labelValue: FieldValue): DataQueryBuilder = {
            this.exclude += labelValue
            this
        }

        private[namespaces] def addInclude(labelValue: FieldValue): DataQueryBuilder = {
            this.include += labelValue
            this
        }

        def build: DataQuery = {
            DataQuery(namespaceName, include.toSet, exclude.toSet)
        }
    }

    private def createCompleteQuery(query: DataQueryBuilder) = new CompleteQuery(query)

    private def createExpectingWhere(query: DataQueryBuilder) = new ExpectingWhere(query)

    private def createExpectingPredicate(query: DataQueryBuilder, fieldName: String) =
        new ExpectingPredicate(query, fieldName)

    class CompleteQuery private[namespaces](query: DataQueryBuilder) {
        def and = createExpectingWhere(query)

        def build = query.build
    }

    class ExpectingWhere private[namespaces](query: DataQueryBuilder) {
        def where(fieldName: String): ExpectingPredicate = createExpectingPredicate(query,
            fieldName)
    }

    class ExpectingPredicate private[namespaces](query: DataQueryBuilder, fieldName: String) {
        def eq(value: String) =
            createCompleteQuery(query.addInclude(FieldValue(fieldName, value)))

        def eq(value: Long) =
            createCompleteQuery(query.addInclude(FieldValue(fieldName, value: java.lang.Long)))

        def eq(value: Double) =
            createCompleteQuery(query.addInclude(FieldValue(fieldName, value: java.lang.Double)))

        def eq(value: Boolean) =
            createCompleteQuery(query.addInclude(FieldValue(fieldName, value: java.lang.Boolean)))

        def neq(value: String) =
            createCompleteQuery(query.addExclude(FieldValue(fieldName, value)))

        def neq(value: Long) =
            createCompleteQuery(query.addExclude(FieldValue(fieldName, value: java.lang.Long)))

        def neq(value: Double) =
            createCompleteQuery(query.addExclude(FieldValue(fieldName, value: java.lang.Double)))

        def neq(value: Boolean) =
            createCompleteQuery(query.addExclude(FieldValue(fieldName, value: java.lang.Boolean)))
    }

    def from(namespaceName: String): ExpectingWhere =
        createExpectingWhere(new DataQueryBuilder(namespaceName))

}

case class DataQuery(namespaceName: String, include: Set[FieldValue], exclude: Set[FieldValue])

