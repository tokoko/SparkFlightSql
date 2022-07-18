package com.tokoko.spark.flight.auth

import org.apache.curator.test.TestingServer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class BasicServerAuthValidatorSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var validator: SparkFlightSqlBasicServerAuthValidator = _

  override def beforeAll(): Unit = {
    validator = new SparkFlightSqlBasicServerAuthValidator(Map(
      "spark.flight.auth.basic.users" -> "test_user:test_password,test_user2:test_password2"
    ))
  }

  test("correct credentials are validated") {
    val token = validator.getToken("test_user2", "test_password2")
    val resp = validator.isValid(token)

    assert(resp.isPresent && resp.get().equals("test_user2"))
  }

  test("incorrect credentials throws an exception") {
    try {
      validator.getToken("test_user", "incorrect_password")
    } catch {
      case e: IllegalArgumentException => assert(e.getMessage.equals("invalid credentials"))
      case _: Exception => assert(false)
    }
  }

  test("calling isValid with random token throws an empty response") {
    val b = new Array[Byte](20)
    Random.nextBytes(b)
    assert(!validator.isValid(b).isPresent)
  }

}
