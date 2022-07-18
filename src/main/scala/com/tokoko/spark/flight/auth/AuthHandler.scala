package com.tokoko.spark.flight.auth

import org.apache.arrow.flight.auth.{BasicServerAuthHandler, ServerAuthHandler}

object AuthHandler {

  def apply(conf: Map[String, String]): ServerAuthHandler = {
    val mode = conf.getOrElse("spark.flight.auth", "none")

    if (mode.equals("none")) {
      ServerAuthHandler.NO_OP
    } else if (mode.equals("basic")) {
      new BasicServerAuthHandler(
        new SparkFlightSqlBasicServerAuthValidator(conf)
      )
    } else null
  }

}
