package com.tokoko.spark.flight.sql

object FilterPatternUtils {

  def matches(str: String, pattern: String): Boolean = {
    val patternRegex = pattern
      .replace("%", ".*")
      .replace("_", ".")

    str.matches(patternRegex)
  }

}
