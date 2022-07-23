package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import com.google.protobuf.ByteString.copyFrom
import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, FlightEndpoint, FlightInfo, FlightProducer, FlightServer, Location, Ticket}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ReadChannel
import org.apache.arrow.vector.ipc.message.MessageSerializer
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.commons.lang.SerializationUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.nodes.GroupMember
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.util.ArrowUtilsExtended
import org.apache.zookeeper.CreateMode

import java.io.{ByteArrayInputStream, IOException}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.UUID.randomUUID
import collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

class ZookeeperSparkFlightManager(conf: Map[String, String]) extends SparkFlightManager {

  private val logger = Logger.getLogger(this.getClass)

  private val connectionString = conf("spark.flight.manager.zookeeper.url")

  private val localFlightBuffer = new mutable.HashMap[ByteString, Statement]

  private val rootAllocator = new RootAllocator()

  private val internalFlightServer = FlightServer.builder(rootAllocator, SparkFlightManager.getNodeInfo(conf).internalLocation,
    new ManagerFlightProducer(localFlightBuffer)
  ).build()

  internalFlightServer.start()

  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  private val zkClient = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
  zkClient.start()

  private val membershipPath = conf("spark.flight.manager.zookeeper.membershipPath")

  private val nodeInfo = SparkFlightManager.getNodeInfo(conf)

  private val nodeId = nodeInfo.publicPort.toString

  ZKPaths.mkdirs(zkClient.getZookeeperClient.getZooKeeper , s"$membershipPath-queries")

  val nodeGroup = new GroupMember(zkClient, membershipPath, nodeId, SerializationUtils.serialize(nodeInfo))
  nodeGroup.start()

  override def getLocation: Location = SparkFlightManager.getLocation(conf)

  def getNodes: List[NodeInfo] = {
    nodeGroup.getCurrentMembers
      .values()
      .asScala
      .map(b => SerializationUtils.deserialize(b).asInstanceOf[NodeInfo])
      .toList
  }

  override def getNodeInfo: NodeInfo = SparkFlightManager.getNodeInfo(conf)

  def addFlight(handle: ByteString): Unit = {
    zkClient.create().withMode(CreateMode.EPHEMERAL)
      .forPath(s"$membershipPath-queries/${handle.toStringUtf8}", "RUNNING".getBytes())
  }

  def setCompleted(handle: ByteString): Unit = {
    zkClient.setData().forPath(s"$membershipPath-queries/${handle.toStringUtf8}", "COMPLETED".getBytes())
  }

  def getStatus(handle: ByteString): String = {
    new String(
      zkClient.getData.forPath(s"$membershipPath-queries/${handle.toStringUtf8}")
    )
  }

  def distributeFlight(handle: ByteString, df: DataFrame): (Schema, List[Location]) = {
    addFlight(handle)


    val arrowSchema = ArrowUtilsExtended.toArrowSchema(df.schema, df.sparkSession.sessionState.conf.sessionLocalTimeZone)

    val abr = ArrowUtilsExtended.convertToArrowBatchRdd(df)
    val jsonSchema = arrowSchema.toJson
    val handleString = handle.toStringUtf8
    val nodes = this.getNodes
    val serverURIs = nodes.map(_.internalLocation).map(_.getUri)

    new Thread(() => {

      abr.foreachPartition(it => {
        val serverLocations = serverURIs.map(uri =>
          Location.forGrpcInsecure(uri.getHost, uri.getPort)
        )

        val rootAllocator = new RootAllocator(Long.MaxValue)

        val clients = serverLocations.map(location => {
          FlightClient.builder(rootAllocator, location).build()
        })

        val schema = Schema.fromJSON(jsonSchema)
        val root = VectorSchemaRoot.create(schema, rootAllocator)
        val descriptor = FlightDescriptor.path(handleString)

        it.foreach(r => {
          try {
            val arb = MessageSerializer.deserializeRecordBatch(
              new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(r)
              )), rootAllocator)
            val vectorLoader = new VectorLoader(root)
            vectorLoader.load(arb)
          } catch {
            case e: IOException => e.printStackTrace()
          }

          val clientListener = clients(new Random().nextInt(clients.size))
            .startPut(descriptor, root, new AsyncPutListener())
          clientListener.putNext()
          clientListener.completed()
        })
      })

      this.setCompleted(handle)
    }).start()

    (arrowSchema, nodes.map(_.publicLocation))
  }

  override def distributeFlight(descriptor: FlightDescriptor,
                                df: DataFrame,
                                ticketBuilder: Array[Byte] => Ticket = handle => new Ticket(handle)
                               ): FlightInfo = {
    val handleBytes = randomUUID.toString.getBytes(StandardCharsets.UTF_8)
    val handle = copyFrom(handleBytes)

    val distFlight =  this.distributeFlight(handle, df)

    val ticket: Ticket = ticketBuilder(handleBytes)

    val endpoints = distFlight._2.map(serverLocation => new FlightEndpoint(ticket, serverLocation))

    new FlightInfo(distFlight._1, descriptor, endpoints.asJava, -1, -1)
  }

  override def streamDistributedFlight(handle: ByteString, listener: FlightProducer.ServerStreamListener): Unit = {
    if (!localFlightBuffer.contains(handle)) {
      localFlightBuffer.put(handle, new Statement)
    }

    val statement = localFlightBuffer(handle)

    if (statement == null) {
      //      logger.warn("Couldn't locate requested statement")
      listener.error(new Exception("Couldn't locate requested statement"))
      return
    }

    while ( {
      statement.getRoot == null && !(this.getStatus(handle) == "COMPLETED")
    }) try {
      //      logger.warn("Waiting for statement VectorSchemaRoot: Sleeping for 1 second")
      Thread.sleep(1000)
    } catch {
      case e: InterruptedException =>
        throw new RuntimeException(e)
    }

    val root = statement.getRoot

    var completed = false

    if (root != null) {
      listener.start(root)

      while (!completed) {
        val batch = statement.nextBatch()
        val loader = new VectorLoader(root)
        if (batch == null) {
          if (this.getStatus(handle) == "COMPLETED") {
            completed = true
          } else {
            try {
              //            logger.warn("Waiting for additional ArrowRecordBatches: Sleeping for 1 second")
              Thread.sleep(1000)
            } catch {
              case e: InterruptedException => throw new RuntimeException(e)
            }
          }
        } else {
          if (!completed) {
            try loader.load(batch)
            catch {
              case ex: Exception =>
                ex.printStackTrace()
                throw ex
            }
            listener.putNext()
          }
        }
      }
    }

    listener.completed()
  }

  override def streamDistributedFlight(ticket: Ticket,
                                       listener: FlightProducer.ServerStreamListener,
                                       handleSelector: Ticket => Array[Byte] = ticket => ticket.getBytes): Unit = {
    streamDistributedFlight(copyFrom(handleSelector(ticket)), listener)
  }

  override def close(): Unit = {
    println("Close Called")
    nodeGroup.close()
    zkClient.close()
    internalFlightServer.close()
  }
}
