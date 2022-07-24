package com.tokoko.spark.flight.auth

import com.tokoko.spark.flight.local.EmbeddedDirectory
import org.apache.arrow.flight.auth.BasicServerAuthHandler.BasicAuthValidator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class LdapBasicAuthValidatorSuite extends AnyFunSuite with BeforeAndAfterAll {
  private var validator: BasicAuthValidator = _
//  private var directory: EmbeddedDirectory = _

  override def beforeAll(): Unit = {

    val directory = new EmbeddedDirectory(Seq("example", "com"))

    directory.addEntry("dc=example,dc=com", Seq("top", "domain", "extensibleObject"), Map("dc" -> "example"))
    directory.addEntry("ou=mathematicians,dc=example,dc=com", Seq("organizationalUnit"), Map("ou" -> "mathematicians"))

    directory.addEntry("cn=user1,ou=mathematicians,dc=example,dc=com", Seq("person"),
      Map("sn" -> "snuser1", "cn" -> "user1", "userPassword" -> "password1")
    )

    directory.addEntry("cn=user2,ou=mathematicians,dc=example,dc=com", Seq("person"),
      Map("sn" -> "snuser2", "cn" -> "user2", "userPassword" -> "password2")
    )

    directory.startLdapServer(9000)

    validator = new LdapBasicAuthValidator(Map(
      "spark.flight.auth.ldap.host" -> "localhost",
      "spark.flight.auth.ldap.port" -> "9000",
      "spark.flight.auth.ldap.baseDN" -> "dc=example,dc=com",
      "spark.flight.auth.ldap.searchFilter" -> "(objectClass=person)",
      "spark.flight.auth.ldap.usernameAttribute" -> "cn",
      "spark.flight.auth.ldap.bindDN" -> "uid=admin,ou=system",
      "spark.flight.auth.ldap.bindPassword" -> "secret"
    ))
  }

  test("correct credentials are validated") {
    val token = validator.getToken("user2", "password2")
    val resp = validator.isValid(token)

    assert(resp.isPresent && resp.get().equals("user2"))
  }

  test("incorrect credentials throws an exception") {
    try {
      validator.getToken("user2", "incorrect_password")
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
