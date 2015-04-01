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


case class GHSEdge(weight: Double) extends Serializable 

object GHSNode extends Serializable {
  def apply(): GHSNode = GHSNode(Link(), Link())
}
case class GHSNode(link1: Link, link2: Link) extends Serializable


case class GHSMessage(link: Link, edgeValue: GHSEdge) extends Serializable {
  var isUnset: Boolean = link.from == -1 
}


object GHS extends Serializable {

  def vertexProg(vid: VertexId, attr: GHSNode, message: GHSMessage) = message.isUnset match {
    case true  => attr
    case false => GHSNode(message.link,  attr.link2)
  }
  
  def sendMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]):  Iterator[(VertexId, GHSMessage)] = {
    if (triplet.srcAttr.link1.from != triplet.dstAttr.link1.from) {
      val outBound = GHSMessage(Link(triplet.srcAttr.link1.from, triplet.dstId), triplet.attr)
      val inBound  = GHSMessage(Link(triplet.dstAttr.link1.from ,triplet.srcId), triplet.attr)
      Iterator((triplet.srcId, outBound), (triplet.dstId, inBound))
    }
    else
      Iterator.empty
  }
  
  def mergeMessage(msg1: GHSMessage, msg2: GHSMessage) = msg1.edgeValue.weight > msg2.edgeValue.weight match {
    case true => msg1
    case _    => msg2
  } 

  def setMinimalEdges(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = {

    val initialMessage = GHSMessage(Link(), GHSEdge(0D))
    val numIter = 1

    graph.pregel(initialMessage,  numIter, EdgeDirection.Both)(
    vprog = vertexProg,
    sendMsg = sendMessage,
    mergeMsg = mergeMessage)
  }

}