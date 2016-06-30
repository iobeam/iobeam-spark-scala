package com.iobeam.spark.streams.model.namespaces

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
  * Test building a data query
  */
class DataQueryTest extends FlatSpec with Matchers with MockitoSugar {

    "Correct includes and excludes" should "be set" in {

        val namespaceName = "nsName"
        val (f1, f2, f3, f4) = ("f1", "f2", "f3", "f4")

        val f1shouldEqual = "foo"
        val f2shouldNotEqual = true
        val f3shouldEqual = 0.1
        val f4shouldNotEqual = 42

        val q = DataQuery.from(namespaceName)
            .where(f1).eq(f1shouldEqual)
            .and
            .where(f2).neq(f2shouldNotEqual)
            .and
            .where(f3).eq(f3shouldEqual)
            .build

        assert(q.namespaceName == namespaceName)
        assert(q.include == Set(FieldValue(f1, f1shouldEqual), FieldValue(f3, f3shouldEqual)))
        assert(q.exclude == Set(FieldValue(f2, f2shouldNotEqual)), FieldValue(f4, f4shouldNotEqual))
    }
}
