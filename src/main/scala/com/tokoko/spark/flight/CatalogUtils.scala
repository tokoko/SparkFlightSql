package com.tokoko.spark.flight

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{FunctionCatalog, Identifier, SupportsNamespaces, TableCatalog}

import java.util
import collection.JavaConverters._

object CatalogUtils {

  /*
    Right now CatalogManager doesn't expose a way to list plugged-in catalogs.
    This is a work-around to get the list from spark configuration directly.
   */

  def listCatalogs(sparkSession: SparkSession): java.util.List[String] = {
    val prefix = "spark.sql.catalog."
    val defaultCatalog = "spark_catalog"

    sparkSession.conf.getAll
      .keySet
      .filter(conf => conf.startsWith(prefix))
      .map(conf => conf.substring(prefix.length))
      .filter(conf => !conf.contains("."))
      .union(Set(defaultCatalog))
      .toList
      .asJava
  }

  def listNamespaces(sparkSession: SparkSession, catalog: String, filterPattern: String): util.List[(String, String)] = {
    val manager = sparkSession.sessionState.catalogManager

    val requestedCatalogs = if (catalog == "") listCatalogs(sparkSession).asScala.toList else List(catalog)

    requestedCatalogs
      .map(c => manager.catalog(c))
      .collect {
        case catalog: SupportsNamespaces => {
          catalog.listNamespaces()
            .map(namespace => (catalog.name(), namespace.head))
        }
        case _ => Array.empty[(String, String)]
      }.flatten
        .filter(namespace => filterPattern == null || FilterPatternUtils.matches(namespace._2, filterPattern))
        .asJava
  }

  def listTables(sparkSession: SparkSession, catalog: String, schemaPattern: String, tablePattern: String): util.List[(String, String, String)] = {
    val manager = sparkSession.sessionState.catalogManager
    val namespaces = listNamespaces(sparkSession, catalog, schemaPattern).asScala

    namespaces
      .map(namespace => (manager.catalog(namespace._1), namespace._2))
      .collect {
        case (catalog: TableCatalog, namespace: String) => {
          catalog.listTables(Array(namespace))
            .map(id => (catalog.name(), id.namespace().head, id.name()))
        }
        case _ => Array.empty[(String, String, String)]
      }.flatten
      .filter(table => tablePattern == null || FilterPatternUtils.matches(table._3, tablePattern))
      .asJava
  }

  def tableExists(sparkSession: SparkSession, catalog: String, schema: String, table: String): Boolean = {
    val manager = sparkSession.sessionState.catalogManager

    manager.isCatalogRegistered(catalog) && manager.catalog(catalog).asInstanceOf[TableCatalog]
      .tableExists(Identifier.of(Array(schema), table))
  }

}
