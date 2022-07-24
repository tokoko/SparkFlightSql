package com.tokoko.spark.flight.auth

class StaticBasicAuthValidator(conf: Map[String, String]) extends TokenGeneratingBasicAuthValidator {
  private val credentials = conf("spark.flight.auth.basic.users")
    .split(",")
    .map(cred => {
      val splitCredentials = cred.split(":")
      (splitCredentials(0), splitCredentials(1))
    }).toMap

  override def authenticate(username: String, password: String): Boolean = {
    credentials.contains(username) && credentials(username).equals(password)
  }
}
