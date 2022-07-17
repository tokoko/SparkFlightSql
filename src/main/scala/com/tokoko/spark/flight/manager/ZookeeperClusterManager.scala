package com.tokoko.spark.flight.manager

import com.google.protobuf.ByteString
import org.apache.arrow.flight._
import org.apache.commons.lang.SerializationUtils
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.nodes.GroupMember
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ZKPaths
import org.apache.log4j.Logger
import org.apache.zookeeper.CreateMode

import java.nio.charset.StandardCharsets
import collection.JavaConverters._

class ZookeeperClusterManager(conf: Map[String, String]) extends ClusterManager {

  private val logger = Logger.getLogger(this.getClass)

  private val connectionString = conf("spark.flight.manager.zookeeper.url")

  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  private val zkClient = CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
  zkClient.start()

  private val membershipPath = conf("spark.flight.manager.zookeeper.membershipPath")

  private val nodeInfo = ClusterManager.getNodeInfo(conf)

  private val nodeId = nodeInfo.publicPort.toString

  ZKPaths.mkdirs(zkClient.getZookeeperClient.getZooKeeper , s"${membershipPath}-queries")

  val nodeGroup = new GroupMember(zkClient, membershipPath, nodeId, SerializationUtils.serialize(nodeInfo))
  nodeGroup.start()

  override def getLocation: Location = ClusterManager.getLocation(conf)

  override def getPeers: List[NodeInfo] = getNodes.filter(ni => ni != nodeInfo)

  override def getNodes: List[NodeInfo] = {
    nodeGroup.getCurrentMembers
      .values()
      .asScala
      .map(b => SerializationUtils.deserialize(b).asInstanceOf[NodeInfo])
      .toList
  }

  override def getInfo: NodeInfo = ClusterManager.getNodeInfo(conf)

  override def addFlight(handle: ByteString): Unit = {
    zkClient.create().withMode(CreateMode.EPHEMERAL)
      .forPath(s"${membershipPath}-queries/${handle.toStringUtf8}", "RUNNING".getBytes())
  }

  override def setCompleted(handle: ByteString): Unit = {
    zkClient.setData().forPath(s"${membershipPath}-queries/${handle.toStringUtf8}", "COMPLETED".getBytes())
  }

  override def getStatus(handle: ByteString): String = {
    new String(
      zkClient.getData.forPath(s"${membershipPath}-queries/${handle.toStringUtf8}")
    )
  }

}
