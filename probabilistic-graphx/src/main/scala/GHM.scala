package org.apache.spark.graphx.probabilistic

/**

Minimum spanning tree implementation using Gallager Humblet Spira algorithm

**/
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.VertexId

object Link extends Serializable {
  def apply():Link = Link(-1L, -1L)
}
case class Link(from: VertexId, to: VertexId) extends Serializable


case class GHMEdge(weight: Double) extends Serializable 

object GHMNode extends Serializable {
  def apply(): GHMNode = GHMNode(Link(), Link())
}
case class GHMNode(link1: Link, link2: Link) extends Serializable


case class GHMMessage(link: Link, edgeValue: GHMEdge) extends Serializable {
  var isUnset: Boolean = link.from == -1 
}


object GHM extends Serializable {

  def vertexProg(vid: VertexId, attr: GHMNode, message: GHMMessage) = message.isUnset match {
    case true  => attr
    case false => GHMNode(message.link,  attr.link2)
  }
  
  def sendMessage(triplet: EdgeTriplet[GHMNode, GHMEdge]):  Iterator[(VertexId, GHMMessage)] = {
    if (triplet.srcAttr.link1.from != triplet.dstAttr.link1.from) {
      val outBound = GHMMessage(Link(triplet.srcAttr.link1.from, triplet.dstId), triplet.attr)
      val inBound  = GHMMessage(Link(triplet.dstAttr.link1.from ,triplet.srcId), triplet.attr)
      Iterator((triplet.srcId, outBound), (triplet.dstId, inBound))
    }
    else
      Iterator.empty
  }
  
  def mergeMessage(msg1: GHMMessage, msg2: GHMMessage) = msg1.edgeValue.weight > msg2.edgeValue.weight match {
    case true => msg1
    case _    => msg2
  } 

  def setMinimalEdges(graph : Graph[GHMNode, GHMEdge]) : Graph[GHMNode, GHMEdge] = {

    val initialMessage = GHMMessage(Link(), GHMEdge(0D))
    val numIter = 1

    graph.pregel(initialMessage,  numIter, EdgeDirection.Both)(
    vprog = vertexProg,
    sendMsg = sendMessage,
    mergeMsg = mergeMessage)
  }

}