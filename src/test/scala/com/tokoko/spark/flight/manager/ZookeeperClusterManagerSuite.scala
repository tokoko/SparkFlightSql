//package com.tokoko.spark.flight.manager
//
//import com.google.protobuf.ByteString
//import org.apache.curator.test.TestingServer
//import org.scalatest.BeforeAndAfterAll
//import org.scalatest.funsuite.AnyFunSuite
//
//class ZookeeperClusterManagerSuite extends AnyFunSuite with BeforeAndAfterAll {
//
//  var zkServer: TestingServer = _
//
//  override def beforeAll(): Unit = {
//    zkServer = new TestingServer(10000, true)
//  }
//
//  test("test single-node cluster") {
//    val manager = ClusterManager.getClusterManager(Map(
//      "spark.flight.host" -> "localhost",
//      "spark.flight.port" -> 1000.toString,
//      "spark.flight.internal.host" -> "localhost",
//      "spark.flight.internal.port" -> 1000.toString,
//      "spark.flight.public.host" -> "localhost",
//      "spark.flight.public.port" -> 1000.toString,
//      "spark.flight.manager" -> "zookeeper",
//      "spark.flight.manager.zookeeper.url" -> "localhost:10000",
//      "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql"
//    ))
//
//    assert(manager.getInfo == NodeInfo("localhost", 1000, "localhost", 1000))
//    assert(manager.getNodes.length == 1)
//    assert(manager.getInfo == manager.getNodes.head)
//    assert(manager.getPeers.isEmpty)
//    manager.close()
//  }
//
//  test("test multiple-node cluster") {
//    val manager = ClusterManager.getClusterManager(Map(
//      "spark.flight.host" -> "localhost",
//      "spark.flight.port" -> 1000.toString,
//      "spark.flight.internal.host" -> "localhost",
//      "spark.flight.internal.port" -> 1000.toString,
//      "spark.flight.public.host" -> "localhost",
//      "spark.flight.public.port" -> 1000.toString,
//      "spark.flight.manager" -> "zookeeper",
//      "spark.flight.manager.zookeeper.url" -> "localhost:10000",
//      "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql2"
//    ))
//
//    val manager2 = ClusterManager.getClusterManager(Map(
//      "spark.flight.host" -> "localhost",
//      "spark.flight.port" -> 1001.toString,
//      "spark.flight.internal.host" -> "localhost",
//      "spark.flight.internal.port" -> 1001.toString,
//      "spark.flight.public.host" -> "localhost",
//      "spark.flight.public.port" -> 1001.toString,
//      "spark.flight.manager" -> "zookeeper",
//      "spark.flight.manager.zookeeper.url" -> "localhost:10000",
//      "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql2"
//    ))
//
//    Thread.sleep(2000) // TODO
//
//    assert(manager.getNodes.length == 2)
//    assert(manager.getNodes == manager2.getNodes)
//    assert(manager.getPeers.head == manager2.getInfo)
//    assert(manager2.getPeers.head == manager.getInfo)
//
//    manager.close()
//    manager2.close()
//  }
//
//  test("test multiple-node cluster flight information exchange") {
//    val manager = ClusterManager.getClusterManager(Map(
//      "spark.flight.host" -> "localhost",
//      "spark.flight.port" -> 1000.toString,
//      "spark.flight.internal.host" -> "localhost",
//      "spark.flight.internal.port" -> 1000.toString,
//      "spark.flight.public.host" -> "localhost",
//      "spark.flight.public.port" -> 1000.toString,
//      "spark.flight.manager" -> "zookeeper",
//      "spark.flight.manager.zookeeper.url" -> "localhost:10000",
//      "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql3"
//    ))
//
//    val manager2 = ClusterManager.getClusterManager(Map(
//      "spark.flight.host" -> "localhost",
//      "spark.flight.port" -> 1001.toString,
//      "spark.flight.internal.host" -> "localhost",
//      "spark.flight.internal.port" -> 1001.toString,
//      "spark.flight.public.host" -> "localhost",
//      "spark.flight.public.port" -> 1001.toString,
//      "spark.flight.manager" -> "zookeeper",
//      "spark.flight.manager.zookeeper.url" -> "localhost:10000",
//      "spark.flight.manager.zookeeper.membershipPath" -> "/spark-flight-sql3"
//    ))
//
//    manager.addFlight(ByteString.copyFromUtf8("1"))
//    assert(manager2.getStatus(ByteString.copyFromUtf8("1")) == "RUNNING")
//    manager.setCompleted(ByteString.copyFromUtf8("1"))
//    assert(manager2.getStatus(ByteString.copyFromUtf8("1")) == "COMPLETED")
//    manager.close()
//    manager2.close()
//  }
//
//
//  override def afterAll(): Unit = {
//    zkServer.close()
//  }
//
//}
