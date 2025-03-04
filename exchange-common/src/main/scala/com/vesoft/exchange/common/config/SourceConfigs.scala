/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.exchange.common.config

import com.vesoft.exchange.common.utils.NebulaUtils

/**
  * Category use to explain the data source which the Spark application could reading.
  */
object SourceCategory extends Enumeration {
  type Type = Value

  val PARQUET = Value("PARQUET")
  val ORC     = Value("ORC")
  val JSON    = Value("JSON")
  val CSV     = Value("CSV")
  val TEXT    = Value("TEXT")

  val HIVE        = Value("HIVE")
  val NEO4J       = Value("NEO4J")
  val JANUS_GRAPH = Value("JANUS GRAPH")
  val MYSQL       = Value("MYSQL")
  val HBASE       = Value("HBASE")
  val MAXCOMPUTE  = Value("MAXCOMPUTE")
  val CLICKHOUSE  = Value("CLICKHOUSE")
  val POSTGRESQL  = Value("POSTGRESQL")
  val ORACLE      = Value("ORACLE")

  val SOCKET = Value("SOCKET")
  val KAFKA  = Value("KAFKA")
  val PULSAR = Value("PULSAR")
}

class SourceCategory

/**
  * DataSourceConfigEntry
  */
sealed trait DataSourceConfigEntry {
  def category: SourceCategory.Value
}

sealed trait FileDataSourceConfigEntry extends DataSourceConfigEntry {
  def path: String
}

sealed trait ServerDataSourceConfigEntry extends DataSourceConfigEntry {
  def sentence: String
}

sealed trait StreamingDataSourceConfigEntry extends DataSourceConfigEntry {
  def intervalSeconds: Int
}

/**
  * FileBaseSourceConfigEntry
  *
  * @param category
  * @param path
  * @param separator
  * @param header
  */
case class FileBaseSourceConfigEntry(override val category: SourceCategory.Value,
                                     override val path: String,
                                     separator: Option[String] = None,
                                     header: Option[Boolean] = None)
    extends FileDataSourceConfigEntry {
  override def toString: String = {
    s"File source path: ${path}, separator: ${separator}, header: ${header}"
  }
}

/**
  * HiveSourceConfigEntry
  *
  * @param sentence
  */
case class HiveSourceConfigEntry(override val category: SourceCategory.Value,
                                 override val sentence: String)
    extends ServerDataSourceConfigEntry {
  require(sentence.trim.nonEmpty)

  override def toString: String = {
    s"Hive source exec: ${sentence}"
  }
}

/**
  * Neo4JSourceConfigEntry
  *
  * @param sentence
  * @param name
  * @param server
  * @param user
  * @param password
  * @param database
  * @param encryption
  * @param parallel
  * @param checkPointPath use save resume data dir path.
  */
case class Neo4JSourceConfigEntry(override val category: SourceCategory.Value,
                                  override val sentence: String,
                                  name: String,
                                  server: String,
                                  user: String,
                                  password: String,
                                  database: Option[String],
                                  encryption: Boolean,
                                  parallel: Int,
                                  checkPointPath: Option[String])
    extends ServerDataSourceConfigEntry {
  require(sentence.trim.nonEmpty && user.trim.nonEmpty && parallel > 0)

  override def toString: String = {
    s"Neo4J source address: ${server}, user: ${user}, password: ${password}, encryption: ${encryption}," +
      s" checkPointPath: ${checkPointPath}, exec: ${sentence}, parallel: ${parallel}, database: ${database}"
  }
}

case class JanusGraphSourceConfigEntry(override val category: SourceCategory.Value,
                                       override val sentence: String,
                                       isEdge: Boolean)
    extends ServerDataSourceConfigEntry {
  override def toString: String = {
    s"Janus graph source"
  }
}

/**
  * MySQLSourceConfigEntry
  *
  * @param host
  * @param port
  * @param database
  * @param table
  * @param user
  * @param password
  * @param sentence
  * @return
  */
case class MySQLSourceConfigEntry(override val category: SourceCategory.Value,
                                  host: String,
                                  port: Int,
                                  database: String,
                                  table: String,
                                  user: String,
                                  password: String,
                                  override val sentence: String)
    extends ServerDataSourceConfigEntry {
  require(
    host.trim.length != 0 && port > 0 && database.trim.length > 0 && table.trim.length > 0 && user.trim.length > 0)

  override def toString: String = {
    s"MySql source host: ${host}, port: ${port}, database: ${database}, table: ${table}, " +
      s"user: ${user}, password: ${password}, sentence: ${sentence}"
  }
}

/**
  * PostgreSQLSourceConfigEntry
  *
  * @param category
  * @param host
  * @param port
  * @param database
  * @param table
  * @param user
  * @param password
  * @param sentence
  */
case class PostgreSQLSourceConfigEntry(override val category: SourceCategory.Value,
                                       host: String,
                                       port: Int,
                                       database: String,
                                       table: String,
                                       user: String,
                                       password: String,
                                       override val sentence: String)
    extends ServerDataSourceConfigEntry {
  require(
    host.trim.length != 0 && port > 0 && database.trim.length > 0 && table.trim.length > 0 && user.trim.length > 0)

  override def toString: String = {
    s"PostgreSql source host: ${host}, port: ${port}, database: ${database}, table: ${table}, " +
      s"user: ${user}, password: ${password}, sentence: ${sentence}"
  }
}

/**
  * TODO: Support more com.vesoft.exchange.common.config item about Kafka Consumer
  *
  * @param server
  * @param topic
  * @param startingOffsets
  * @param maxOffsetsPerTrigger
  */
case class KafkaSourceConfigEntry(override val category: SourceCategory.Value,
                                  override val intervalSeconds: Int,
                                  server: String,
                                  topic: String,
                                  startingOffsets: String,
                                  maxOffsetsPerTrigger: Option[Long] = None)
    extends StreamingDataSourceConfigEntry {
  require(server.trim.nonEmpty && topic.trim.nonEmpty)

  override def toString: String = {
    s"Kafka source server: ${server} topic:${topic} startingOffsets:${startingOffsets} maxOffsetsPerTrigger:${maxOffsetsPerTrigger}"
  }
}

/**
  * PulsarSourceConfigEntry
  *
  * @param serviceUrl
  * @param adminUrl use to get data schema.
  * @param options
  * @return
  */
case class PulsarSourceConfigEntry(override val category: SourceCategory.Value,
                                   override val intervalSeconds: Int,
                                   serviceUrl: String,
                                   adminUrl: String,
                                   options: Map[String, String])
    extends StreamingDataSourceConfigEntry {
  require(serviceUrl.trim.nonEmpty && adminUrl.trim.nonEmpty && intervalSeconds >= 0)
  require(options.keys.count(key => List("topic", "topics", "topicsPattern").contains(key)) == 1)

  override def toString: String = {
    s"Pulsar source service url: ${serviceUrl} admin url: ${adminUrl} options: ${options}"
  }
}

/**
  * HBaseSourceConfigEntry
  *
  */
case class HBaseSourceConfigEntry(override val category: SourceCategory.Value,
                                  host: String,
                                  port: String,
                                  table: String,
                                  columnFamily: String,
                                  fields: List[String])
    extends ServerDataSourceConfigEntry() {

  require(host.trim.length != 0 && port.trim.length != 0 && NebulaUtils
    .isNumic(port.trim) && table.trim.length > 0 && table.trim.length > 0 && columnFamily.trim.length > 0)

  override val sentence: String = null

  override def toString: String = {
    s"HBase source host: $host, port: $port, table: $table"
  }
}

/**
  * MaxComputeConfigEntry
  */
case class MaxComputeConfigEntry(override val category: SourceCategory.Value,
                                 odpsUrl: String,
                                 tunnelUrl: String,
                                 table: String,
                                 project: String,
                                 accessKeyId: String,
                                 accessKeySecret: String,
                                 partitionSpec: String,
                                 numPartitions: String,
                                 override val sentence: String)
    extends ServerDataSourceConfigEntry {
  require(
    !odpsUrl.trim.isEmpty && !tunnelUrl.trim.isEmpty && !table.trim.isEmpty && !project.trim.isEmpty
      && !accessKeyId.trim.isEmpty && !accessKeySecret.trim.isEmpty)

  override def toString: String = {
    s"MaxCompute source {odpsUrl: $odpsUrl, tunnelUrl: $tunnelUrl, table: $table, project: $project, " +
      s"keyId: $accessKeyId, keySecret: $accessKeySecret, partitionSpec:$partitionSpec, " +
      s"numPartitions:$numPartitions, sentence:$sentence}"
  }

}

/**
  * ClickHouseConfigEntry
  */
case class ClickHouseConfigEntry(override val category: SourceCategory.Value,
                                 url: String,
                                 user: String,
                                 passwd: String,
                                 numPartition: String,
                                 table: String,
                                 override val sentence: String)
    extends ServerDataSourceConfigEntry {
  override def toString: String = {
    s"ClickHouse source {url:$url, user:$user, passwd:$passwd, numPartition:$numPartition, table:$table, sentence:$sentence}"
  }
}

/**
  * OracleConfigEntry
  */
case class OracleConfigEntry(override val category: SourceCategory.Value,
                             url: String,
                             driver: String,
                             user: String,
                             passwd: String,
                             table: String,
                             override val sentence: String)
    extends ServerDataSourceConfigEntry {
  override def toString: String = {
    s"Oracle source {url:$url, driver:$driver, user:$user, passwd:$passwd, table:$table, sentence:$sentence}"
  }
}
