package com.tokoko.spark.flight

import org.scalatest.funsuite.AnyFunSuite

class FilterPatternTestSuite extends AnyFunSuite{

  test("exact matches work") {
    assert(FilterPatternUtils.matches("FlightSql", "FlightSql"))
  }

  test("% wildcard matches") {
    assert(FilterPatternUtils.matches("FlightSql", "%Sql"))
    assert(!FilterPatternUtils.matches("FlightSql", "Spark%Sql"))
  }

  test("_ wildcard matches") {
    assert(FilterPatternUtils.matches("FlightSql", "Fligh_Sql"))
    assert(!FilterPatternUtils.matches("FlightSql", "_Sql"))
  }



}
