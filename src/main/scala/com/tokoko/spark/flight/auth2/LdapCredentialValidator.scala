package com.tokoko.spark.flight.auth2

import org.apache.arrow.flight.auth2.CallHeaderAuthenticator
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator.CredentialValidator
import org.apache.directory.api.ldap.model.message.SearchScope
import org.apache.directory.api.ldap.model.name.Dn
import org.apache.directory.api.util.Network
import org.apache.directory.ldap.client.api.{LdapConnectionConfig, LdapNetworkConnection}

class LdapCredentialValidator(conf: Map[String, String]) extends CredentialValidator {

  private val ldapHost = conf.getOrElse("spark.flight.auth.ldap.host", Network.LOOPBACK_HOSTNAME)
  private val ldapPort = Integer.parseInt(conf.getOrElse("spark.flight.auth.ldap.port", "9000"))
  private val baseDN = conf.getOrElse("spark.flight.auth.ldap.baseDN", "dc=example,dc=com")
  private val searchFilter = conf.getOrElse("spark.flight.auth.ldap.searchFilter", "(objectClass=person)")
  private val usernameAttribute = conf.getOrElse("spark.flight.auth.ldap.usernameAttribute", "cn")
  private val bindDN = conf.getOrElse("spark.flight.auth.ldap.bindDN", "uid=admin,ou=system")
  private val bindPassword = conf.getOrElse("spark.flight.auth.ldap.bindPassword", "secret")
  ////connectionMode

  private val ldapConfig = new LdapConnectionConfig()
  ldapConfig.setLdapHost(ldapHost)
  ldapConfig.setUseSsl(false)
  ldapConfig.setLdapPort(ldapPort)

  private val connection = new LdapNetworkConnection(ldapConfig)
  connection.bind(bindDN, bindPassword)


  override def validate(username: String, password: String): CallHeaderAuthenticator.AuthResult = {

    val finalSearchFilter = s"(|$searchFilter($usernameAttribute=$username)(userPassword=$password))"

    val entryCursor = connection.search(
      new Dn(baseDN),
      finalSearchFilter,
      SearchScope.SUBTREE)

    if(entryCursor.iterator().hasNext) {
      new LdapAuthResult(username)
    } else {
      throw new IllegalArgumentException("invalid credentials")
    }

  }

  class LdapAuthResult(username: String) extends CallHeaderAuthenticator.AuthResult {
    override def getPeerIdentity: String = username
  }

}
