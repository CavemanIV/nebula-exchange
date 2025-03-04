/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.exchange.reader

import com.google.common.collect.Maps
import com.vesoft.exchange.common.config.{
  ClickHouseConfigEntry,
  HBaseSourceConfigEntry,
  HiveSourceConfigEntry,
  JanusGraphSourceConfigEntry,
  MaxComputeConfigEntry,
  MySQLSourceConfigEntry,
  Neo4JSourceConfigEntry,
  OracleConfigEntry,
  PostgreSQLSourceConfigEntry,
  ServerDataSourceConfigEntry
}
import com.vesoft.exchange.common.utils.HDFSUtils
import com.vesoft.nebula.exchange.utils.Neo4jUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.{
  ClusterCountMapReduce,
  PeerPressureVertexProgram
}
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory
import org.neo4j.driver.internal.types.{TypeConstructor, TypeRepresentation}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import org.neo4j.spark.dataframe.CypherTypes
import org.neo4j.spark.utils.Neo4jSessionAwareIterator
import org.neo4j.spark.{Executor, Neo4jConfig}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * ServerBaseReader is the abstract class of
  * It include a spark session and a sentence which will sent to service.
  * @param session
  * @param sentence
  */
abstract class ServerBaseReader(override val session: SparkSession, val sentence: String)
    extends Reader {

  override def close(): Unit = {
    session.close()
  }
}

/**
  * HiveReader extends the @{link ServerBaseReader}.
  * The HiveReader reading data from Apache Hive via sentence.
  * @param session
  * @param hiveConfig
  */
class HiveReader(override val session: SparkSession, hiveConfig: HiveSourceConfigEntry)
    extends ServerBaseReader(session, hiveConfig.sentence) {
  override def read(): DataFrame = {
    session.sql(sentence)
  }
}

/**
  * The MySQLReader extends the ServerBaseReader.
  * The MySQLReader reading data from MySQL via sentence.
  *
  * @param session
  * @param mysqlConfig
  */
class MySQLReader(override val session: SparkSession, mysqlConfig: MySQLSourceConfigEntry)
    extends ServerBaseReader(session, mysqlConfig.sentence) {
  override def read(): DataFrame = {
    val url =
      s"jdbc:mysql://${mysqlConfig.host}:${mysqlConfig.port}/${mysqlConfig.database}?useUnicode=true&characterEncoding=utf-8"
    val df = session.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", mysqlConfig.table)
      .option("user", mysqlConfig.user)
      .option("password", mysqlConfig.password)
      .load()
    if (sentence != null) {
      df.createOrReplaceTempView(mysqlConfig.table)
      session.sql(sentence)
    } else {
      df
    }
  }
}

/**
  * The PostgreSQLReader extends the ServerBaseReader
  */
class PostgreSQLReader(override val session: SparkSession,
                       postgreConfig: PostgreSQLSourceConfigEntry)
    extends ServerBaseReader(session, postgreConfig.sentence) {
  override def read(): DataFrame = {
    val url =
      s"jdbc:postgresql://${postgreConfig.host}:${postgreConfig.port}/${postgreConfig.database}"
    val df = session.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", url)
      .option("dbtable", postgreConfig.table)
      .option("user", postgreConfig.user)
      .option("password", postgreConfig.password)
      .load()
    if (sentence != null) {
      df.createOrReplaceTempView(postgreConfig.table)
      session.sql(sentence)
    } else {
      df
    }
  }
}

/**
  * Neo4JReader extends the ServerBaseReader
  * this reader support checkpoint by sacrificing performance
  * @param session
  * @param neo4jConfig
  */
class Neo4JReader(override val session: SparkSession, neo4jConfig: Neo4JSourceConfigEntry)
    extends ServerBaseReader(session, neo4jConfig.sentence)
    with CheckPointSupport {

  @transient lazy private val LOG = Logger.getLogger(this.getClass)

  override def read(): DataFrame = {
    if (!neo4jConfig.sentence.toUpperCase.contains("ORDER"))
      LOG.warn(
        "We strongly suggest the cypher sentence must use `order by` clause.\n" +
          "Because the `skip` clause in partition no guarantees are made on the order of the result unless the query specifies the ORDER BY clause")
    val totalCount: Long = {
      val returnIndex   = neo4jConfig.sentence.toUpperCase.lastIndexOf("RETURN") + "RETURN".length
      val countSentence = neo4jConfig.sentence.substring(0, returnIndex) + " count(*)"
      val driver =
        GraphDatabase.driver(s"${neo4jConfig.server}",
                             AuthTokens.basic(neo4jConfig.user, neo4jConfig.password))
      val neo4JSession = driver.session()
      neo4JSession.run(countSentence).single().get(0).asLong()
    }

    val offsets =
      getOffsets(totalCount, neo4jConfig.parallel, neo4jConfig.checkPointPath, neo4jConfig.name)
    LOG.info(s"${neo4jConfig.name} offsets: ${offsets.mkString(",")}")
    if (offsets.forall(_.size == 0L)) {
      LOG.warn(s"${neo4jConfig.name} already write done from check point.")
      return session.createDataFrame(session.sparkContext.emptyRDD[Row], new StructType())
    }

    val config = Neo4jConfig(neo4jConfig.server,
                             neo4jConfig.user,
                             Some(neo4jConfig.password),
                             neo4jConfig.database,
                             neo4jConfig.encryption)

    val rdd = session.sparkContext
      .parallelize(offsets, offsets.size)
      .flatMap(offset => {
        if (neo4jConfig.checkPointPath.isDefined) {
          val path =
            s"${neo4jConfig.checkPointPath.get}/${neo4jConfig.name}.${TaskContext.getPartitionId()}"
          HDFSUtils.saveContent(path, offset.start.toString)
        }
        val query     = s"${neo4jConfig.sentence} SKIP ${offset.start} LIMIT ${offset.size}"
        val result    = new Neo4jSessionAwareIterator(config, query, Maps.newHashMap(), false)
        val fields    = if (result.hasNext) result.peek().keys().asScala else List()
        val neo4jType = new TypeRepresentation(TypeConstructor.STRING)
        val schema =
          if (result.hasNext)
            StructType(
              fields
                .map(k => (k, neo4jType))
                .map(keyType => CypherTypes.field(keyType)))
          else new StructType()
        result.map(record => {
          val row = new Array[Any](record.keys().size())
          for (i <- row.indices)
            row.update(i, Executor.convert(Neo4jUtils.convertNeo4jData(record.get(i))))
          new GenericRowWithSchema(values = row, schema).asInstanceOf[Row]
        })
      })

    if (rdd.isEmpty())
      throw new RuntimeException(
        "Please check your cypher sentence. because use it search nothing!")
    val schema = rdd.repartition(1).first().schema
    session.createDataFrame(rdd, schema)
  }
}

/**
  * JanusGraphReader extends the link ServerBaseReader
  * @param session
  * @param janusGraphConfig
  */
class JanusGraphReader(override val session: SparkSession,
                       janusGraphConfig: JanusGraphSourceConfigEntry)
    extends ServerBaseReader(session, "")
    with CheckPointSupport {

  override def read(): DataFrame = {
    val graph = GraphFactory.open("conf/hadoop/hadoop-gryo.properties")
    graph.configuration().setProperty("gremlin.hadoop.graphWriter", classOf[PersistedOutputRDD])
    graph.configuration().setProperty("gremlin.spark.persistContext", true)

    val result = graph
      .compute(classOf[SparkGraphComputer])
      .program(PeerPressureVertexProgram.build().create(graph))
      .mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create())
      .submit()
      .get()

    if (janusGraphConfig.isEdge) {
      result.graph().edges()
    } else {
      result.graph().variables().asMap()
    }
    null
  }
}

/**
  *
  * @param session
  * @param nebulaConfig
  */
class NebulaReader(override val session: SparkSession, nebulaConfig: ServerDataSourceConfigEntry)
    extends ServerBaseReader(session, nebulaConfig.sentence) {
  override def read(): DataFrame = ???
}

/**
  * HBaseReader extends [[ServerBaseReader]]
  *
  */
class HBaseReader(override val session: SparkSession, hbaseConfig: HBaseSourceConfigEntry)
    extends ServerBaseReader(session, null) {

  private[this] val LOG = Logger.getLogger(this.getClass)

  override def read(): DataFrame = {
    val cf       = hbaseConfig.columnFamily
    val scanConf = HBaseConfiguration.create()
    scanConf.set("hbase.zookeeper.quorum", hbaseConfig.host)
    scanConf.set("hbase.zookeeper.property.clientPort", hbaseConfig.port)
    scanConf.set(TableInputFormat.INPUT_TABLE, hbaseConfig.table)
    hbaseConfig.fields.filter(field => !field.equalsIgnoreCase("rowkey"))
    scanConf.set(TableInputFormat.SCAN_COLUMNS,
                 hbaseConfig.fields
                   .filter(field => !field.equalsIgnoreCase("rowkey"))
                   .map(field => s"$cf:$field")
                   .mkString(" "))
    val fields = hbaseConfig.fields

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = session.sparkContext.newAPIHadoopRDD(
      scanConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val rowRDD = hbaseRDD.map(row => {
      val values: ListBuffer[String] = new ListBuffer[String]
      val result: Result             = row._2

      for (i <- fields.indices) {
        if (fields(i).equalsIgnoreCase("rowkey")) {
          values += Bytes.toString(result.getRow)
        } else {
          values += Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(fields(i))))
        }
      }
      Row.fromSeq(values.toList)
    })
    val schema = StructType(
      fields.map(field => DataTypes.createStructField(field, DataTypes.StringType, true)))
    val dataFrame = session.createDataFrame(rowRDD, schema)
    dataFrame
  }
}

/**
  * MaxCompute Reader
  */
class MaxcomputeReader(override val session: SparkSession, maxComputeConfig: MaxComputeConfigEntry)
    extends ServerBaseReader(session, maxComputeConfig.sentence) {

  override def read(): DataFrame = {
    var dfReader = session.read
      .format("org.apache.spark.aliyun.odps.datasource")
      .option("odpsUrl", maxComputeConfig.odpsUrl)
      .option("tunnelUrl", maxComputeConfig.tunnelUrl)
      .option("table", maxComputeConfig.table)
      .option("project", maxComputeConfig.project)
      .option("accessKeyId", maxComputeConfig.accessKeyId)
      .option("accessKeySecret", maxComputeConfig.accessKeySecret)
      .option("numPartitions", maxComputeConfig.numPartitions)

    // if use partition read
    if (maxComputeConfig.partitionSpec != null) {
      dfReader = dfReader.option("partitionSpec", maxComputeConfig.partitionSpec)
    }

    val df = dfReader.load()
    import session._
    if (maxComputeConfig.sentence == null) {
      df
    } else {
      df.createOrReplaceTempView(s"${maxComputeConfig.table}")
      session.sql(maxComputeConfig.sentence)
    }
  }
}

/**
  * Clickhouse reader
  */
class ClickhouseReader(override val session: SparkSession,
                       clickHouseConfigEntry: ClickHouseConfigEntry)
    extends ServerBaseReader(session, clickHouseConfigEntry.sentence) {
  Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

  override def read(): DataFrame = {
    val df = session.read
      .format("jdbc")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("url", clickHouseConfigEntry.url)
      .option("user", clickHouseConfigEntry.user)
      .option("password", clickHouseConfigEntry.passwd)
      .option("numPartitions", clickHouseConfigEntry.numPartition)
      .option("query", clickHouseConfigEntry.sentence)
      .load()
    df
  }
}

/**
  * Oracle reader
  */
class OracleReader(override val session: SparkSession, oracleConfig: OracleConfigEntry)
    extends ServerBaseReader(session, oracleConfig.sentence) {
  Class.forName(oracleConfig.driver)
  override def read(): DataFrame = {
    var df = session.read
      .format("jdbc")
      .option("url", oracleConfig.url)
      .option("dbtable", oracleConfig.table)
      .option("user", oracleConfig.user)
      .option("password", oracleConfig.passwd)
      .option("driver", oracleConfig.driver)
      .load()

    if (oracleConfig.sentence != null) {
      val tableName = if (oracleConfig.table.contains(".")) {
        oracleConfig.table.split("\\.")(1)
      } else oracleConfig.table
      df.createOrReplaceTempView(tableName)
      df = session.sql(sentence)
    }
    df
  }
}
