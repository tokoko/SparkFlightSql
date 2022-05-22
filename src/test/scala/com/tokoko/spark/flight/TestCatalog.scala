package com.tokoko.spark.flight

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import collection.JavaConverters._

import java.util

class TestCatalog extends TableCatalog with SupportsNamespaces {

  private var catalogName: String = _

  private var databases: CaseInsensitiveStringMap = _

  def listTables(namespace: Array[String]): Array[Identifier] = {
    databases.get(namespace.head)
      .split(",")
      .map(t => Identifier.of(namespace, t))
  }

  def loadTable(ident: Identifier): Table = ???

  def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = ???

  def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  def dropTable(ident: Identifier): Boolean = ???

  def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
    databases = options
  }

  def name(): String = catalogName

  def listNamespaces(): Array[Array[String]] = {
    databases
      .keySet()
      .asScala
      .map(db => Array(db))
      .toArray
  }

  def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    listNamespaces()
  }

  def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = ???

  def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = ???

  def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  def dropNamespace(namespace: Array[String]): Boolean = ???
}
