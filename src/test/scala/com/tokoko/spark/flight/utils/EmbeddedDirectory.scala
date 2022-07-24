package com.tokoko.spark.flight.utils

import org.apache.directory.server.core.factory.{DefaultDirectoryServiceFactory, JdbmPartitionFactory}
import org.apache.directory.server.ldap.LdapServer
import org.apache.directory.server.protocol.shared.transport.TcpTransport

import java.io.File

class EmbeddedDirectory(rootDomains: Seq[String]) {

  private val directoryServiceFactory = new DefaultDirectoryServiceFactory()
  directoryServiceFactory.init(rootDomains.mkString("-"))

  private val directoryService = directoryServiceFactory.getDirectoryService

  private val basePartition = new JdbmPartitionFactory().createPartition(
    directoryService.getSchemaManager,
    directoryService.getDnFactory,
    rootDomains.mkString("-"),
    rootDomains.map(d => s"dc=$d").mkString(","),
    1000,
    new File(directoryService.getInstanceLayout.getPartitionsDirectory, rootDomains.mkString("-"))
  )

  directoryService.addPartition(basePartition)

  directoryService.startup()

  def addEntry(dn: String, objectClass: Seq[String], attrMap: Map[String, String]): Unit = {
    val entry = directoryService.newEntry(directoryService.getDnFactory.create(dn))
    entry.add("objectClass", objectClass:_*)
    attrMap.foreach(e => entry.add(e._1, e._2))

    val session = directoryService.getAdminSession
    try {
      session.add(entry);
    } finally {
      session.unbind();
    }
  }

  def startLdapServer(port: Int): Unit = {
    val server = new LdapServer()
    server.setDirectoryService(directoryService)
    server.setTransports(new TcpTransport(port))
    server.start()
  }


}
