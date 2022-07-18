package com.tokoko.spark.flight.auth

import org.apache.arrow.flight.auth.BasicServerAuthHandler
import java.util.Optional
import scala.collection.mutable
import scala.util.Random

class SparkFlightSqlBasicServerAuthValidator(conf: Map[String, String]) extends BasicServerAuthHandler.BasicAuthValidator {
  private val validTokens = new mutable.HashMap[String, String]()
  private val validUsers = new mutable.HashMap[String, Array[Byte]]()

  private val credentials = conf("spark.flight.auth.basic.users")
    .split(",")
    .map(cred => {
      val splitCredentials = cred.split(":")
      (splitCredentials(0), splitCredentials(1))
    }).toMap

  override def getToken(username: String, password: String): Array[Byte] = {
    if (credentials.contains(username) && credentials(username).equals(password)) {
      validUsers.getOrElseUpdate(username, {
        val randomToken: String = Random.alphanumeric take 10 mkString "" // Random.nextString(10)
        validTokens.put(randomToken, username)
        randomToken.getBytes
        })
    } else {
      throw new IllegalArgumentException("invalid credentials")
    }
  }

  override def isValid(token: Array[Byte]): Optional[String] = {
    if (token != null && validTokens.contains(new String(token))) {
      Optional.ofNullable(validTokens.get(new String(token)).orNull)
    } else {
      Optional.empty()
    }
  }

}
