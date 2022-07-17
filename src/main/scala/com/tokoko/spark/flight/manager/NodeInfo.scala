package com.tokoko.spark.flight.manager

import org.apache.arrow.flight.Location

//object NodeInfo {
//  def serialize(nodeInfo: NodeInfo): String = {
//    s"${nodeInfo.internalHost},${nodeInfo.internalPort},${nodeInfo.publicHost},${nodeInfo.publicPort}"
//  }
//
//  def deserialize(string: String): NodeInfo = {
//    println("DESERIALIZING " + string)
//    val split = string.split(",")
//
//    NodeInfo(
//      split(0),
//      Integer.parseInt(split(1)),
//      split(2),
//      Integer.parseInt(split(3))
//    )
//  }
//
//}

case class NodeInfo(internalHost: String,
                    internalPort: Int,
                    publicHost: String,
                    publicPort: Int) extends Serializable {
  def internalLocation: Location = Location.forGrpcInsecure(internalHost, internalPort)
  def publicLocation: Location = Location.forGrpcInsecure(publicHost, publicPort)
}
