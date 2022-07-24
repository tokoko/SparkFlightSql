package com.tokoko.spark.flight.auth

import org.apache.arrow.flight.auth.BasicServerAuthHandler

import java.util.Optional
import scala.collection.mutable
import scala.util.Random

abstract class TokenGeneratingBasicAuthValidator extends BasicServerAuthHandler.BasicAuthValidator {
  private val validTokens = new mutable.HashMap[String, String]()
  private val validUsers = new mutable.HashMap[String, Array[Byte]]()

  def authenticate(username: String, password: String): Boolean

  override def getToken(username: String, password: String): Array[Byte] = {
    if (authenticate(username, password)) {
      validUsers.getOrElseUpdate(username, {
        val randomToken: String = Random.alphanumeric take 10 mkString ""
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
