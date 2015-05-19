/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Compute the mwst thanks to message passing algorithm        //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object Fragment extends Serializable
{
	def apply():Fragment = Fragment(-1L, -1L, 0D)
}

object AddedLink extends Serializable 
{
	def apply():AddedLink = AddedLink(-1L, -1L)
}

case class Fragment(id: VertexId, lastFragmentId: VertexId, minWeight : Double = 0D) extends Serializable

case class AddedLink(from: VertexId, to: VertexId) extends Serializable

case class GHSEdge(weight: Double) extends Serializable

object GHSNode extends Serializable 
{
	def apply(): GHSNode = GHSNode(Fragment(), AddedLink())
}
case class GHSNode(fragment: Fragment, addedLink: AddedLink) extends Serializable

case class GHSMessage(node: GHSNode, edgeValue: GHSEdge) extends Serializable 
{
	var isUnset: Boolean = node.fragment.id == -1L
}

case class UpdateMessage(newId : VertexId = -1L) extends Serializable
{
	var isUnset: Boolean = newId == -1L
}

object GHS extends Serializable
{
	
	/////////////////////////////////////////////First phase////////////////////////////////////////////
	
	// => compute the minimum edges from a fragment to another keeping the corresponding fragment id
	def vertexProg(vid: VertexId, attr: GHSNode, message: GHSMessage) = message.isUnset match 
	{
		case true => attr
		case false => GHSNode(Fragment(attr.fragment.id, attr.fragment.id, message.edgeValue.weight), message.node.addedLink)
	}
	
	def sendMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, GHSMessage)] = 
	{
		if (triplet.srcAttr.fragment.id != triplet.dstAttr.fragment.id) 
		{
			val updatedSrcFragment = GHSMessage(GHSNode(Fragment(triplet.dstAttr.fragment.id, triplet.dstId), AddedLink(triplet.srcId, triplet.dstId)), triplet.attr)
			val updatedDstFragment  = GHSMessage(GHSNode(Fragment(triplet.srcAttr.fragment.id ,triplet.srcId), AddedLink(triplet.srcId, triplet.dstId)), triplet.attr)
			Iterator((triplet.srcId, updatedSrcFragment), (triplet.dstId, updatedDstFragment))
		}
		else
			Iterator.empty
	}
	
	def mergeMessage(msg1: GHSMessage, msg2: GHSMessage) =
	{
		if (msg1.edgeValue.weight < msg2.edgeValue.weight) msg1
		else if	(msg1.edgeValue.weight == msg2.edgeValue.weight) 
		{
			if (msg1.node.addedLink.from < msg2.node.addedLink.from) msg1
			else if (msg1.node.addedLink.from == msg2.node.addedLink.from)
			{
				if (msg1.node.addedLink.to < msg2.node.addedLink.to) msg1
				else msg2
			}
			else msg2
		}
		else msg2
	}	
		
	def setMinFragmentId(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	{
		val initialMessage = GHSMessage(GHSNode(), GHSEdge(0D))
		val numIter = 1
	
		graph.pregel(initialMessage, numIter, EdgeDirection.Both)(
		vprog = vertexProg,
		sendMsg = sendMessage,
		mergeMsg = mergeMessage)
	}
	
	
	/////////////////////////////////////////////Second phase////////////////////////////////////////////
	
	// => compute the minimum edges in a fragment in order to link fragment with the minimum edge in the fragment
	def fragmentProg(vid: VertexId, attr: GHSNode, message: GHSMessage) = message.isUnset match 
	{
		case true => attr
		case false =>
		{
			if (message.edgeValue.weight < attr.fragment.minWeight)
				GHSNode(Fragment(attr.fragment.id, attr.fragment.lastFragmentId, message.edgeValue.weight), AddedLink())
			else if (message.edgeValue.weight ==  attr.fragment.minWeight)
			{
				if(message.node.fragment.lastFragmentId < vid)
					GHSNode(Fragment(attr.fragment.id, attr.fragment.lastFragmentId, message.edgeValue.weight), AddedLink())
				else
					GHSNode(Fragment(attr.fragment.id, attr.fragment.lastFragmentId, attr.fragment.minWeight), attr.addedLink)
			}
			else
				GHSNode(Fragment(attr.fragment.id, attr.fragment.lastFragmentId, attr.fragment.minWeight), attr.addedLink)
		} 
	}

	def sendInFragmentMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, GHSMessage)] = 
	{
		if (triplet.srcAttr.fragment.id == triplet.dstAttr.fragment.id) 
		{
				val updatedSrcFragment = GHSMessage(GHSNode(Fragment(triplet.dstAttr.fragment.id, triplet.dstId), AddedLink()), GHSEdge(triplet.dstAttr.fragment.minWeight))
				val updatedDstFragment = GHSMessage(GHSNode(Fragment(triplet.srcAttr.fragment.id ,triplet.srcId), AddedLink()), GHSEdge(triplet.srcAttr.fragment.minWeight))
				Iterator((triplet.srcId, updatedSrcFragment), (triplet.dstId, updatedDstFragment))				
		}
		else
			Iterator.empty
	}
		
	def computeMinInFragment(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	{
		val initialMessage = GHSMessage(GHSNode(), GHSEdge(0D))
		val numIter = 1
		
		graph.pregel(initialMessage, numIter, EdgeDirection.Both)(
		vprog = fragmentProg,
		sendMsg = sendInFragmentMessage,
		mergeMsg = mergeMessage)
	}
	
	
	/////////////////////////////////////////////Third phase////////////////////////////////////////////
	
	// => Update the fragment Id of the elements linking two fragments
	def updateFragId(vid: VertexId, attr: GHSNode, message: UpdateMessage) = message.isUnset match 
	{
		case true => attr
		case false => GHSNode(Fragment(message.newId, attr.fragment.lastFragmentId), attr.addedLink)
	}

	def sendUpdateMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, UpdateMessage)] = 
	{
		if (triplet.srcAttr.fragment.id < triplet.dstAttr.fragment.id)
		{
			if(triplet.srcAttr.addedLink.to == triplet.dstId)
				Iterator((triplet.dstId, UpdateMessage(triplet.srcAttr.fragment.id)))
			else if(triplet.dstAttr.addedLink.from == triplet.srcId)
				Iterator((triplet.dstId, UpdateMessage(triplet.srcAttr.fragment.id)))
			else
				Iterator.empty
		}
		else if (triplet.srcAttr.fragment.id > triplet.dstAttr.fragment.id)
		{
			if(triplet.dstAttr.addedLink.from == triplet.srcId)
				Iterator((triplet.srcId, UpdateMessage(triplet.dstAttr.fragment.id)))
			else if(triplet.srcAttr.addedLink.to == triplet.dstId)
				Iterator((triplet.srcId, UpdateMessage(triplet.dstAttr.fragment.id)))
			else
				Iterator.empty
		}
		else
			Iterator.empty
	}
		
	def mergeUpdateMessage(msg1: UpdateMessage, msg2: UpdateMessage) = msg1.newId < msg2.newId match
	{
		case true => msg1
		case _ => msg2
	}	
	
	def updateFragmentId(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	{
		val initialMessage = UpdateMessage()
		
		graph.pregel(initialMessage)(
		vprog = updateFragId,
		sendMsg = sendUpdateMessage,
		mergeMsg = mergeUpdateMessage)
	}
	
	
	/////////////////////////////////////////////Fourth phase////////////////////////////////////////////
	
	// => Update the id of the nodes of a fragment
	def updateFrag(vid: VertexId, attr: GHSNode, message: UpdateMessage) = message.isUnset match 
	{
		case true => attr
		case false => GHSNode(Fragment(message.newId, message.newId), attr.addedLink)
	}

	def sendUpdate(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, UpdateMessage)] = 
	{
		if (triplet.srcAttr.fragment.id == triplet.dstAttr.fragment.lastFragmentId && triplet.dstAttr.fragment.id < triplet.srcAttr.fragment.id)
			Iterator((triplet.srcId, UpdateMessage(triplet.dstAttr.fragment.id)))
		else if (triplet.dstAttr.fragment.id == triplet.srcAttr.fragment.lastFragmentId && triplet.srcAttr.fragment.id < triplet.dstAttr.fragment.id)
			Iterator((triplet.dstId, UpdateMessage(triplet.srcAttr.fragment.id)))
		else
			Iterator.empty
	}	
	
	def updateFragment(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	{
		val initialMessage = UpdateMessage()
		
		graph.pregel(initialMessage)(
		vprog = updateFrag,
		sendMsg = sendUpdate,
		mergeMsg = mergeUpdateMessage)
	}
	
	/////////////////////////////////////////////GHS recursive algorithm////////////////////////////////////////////

	def isEmpty[T](rdd : RDD[T]) = 
	{
		rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_) 
	}
	
	def GHSrec(graph : Graph[GHSNode, GHSEdge], edgeAdded : RDD[Edge[GHSEdge]]) : Graph[GHSNode, GHSEdge] = 
	{
		// update the fragment Id and check which is the best edges in a fragment
		val minFragmentEdgeGraph = computeMinInFragment(graph).cache()
		// compute the edges to be kept
		val edges = minFragmentEdgeGraph.vertices.map{ case(vid,node) => Edge(node.addedLink.from, node.addedLink.to, GHSEdge(0D))}.filter{link => link.srcId != -1L}
		val newSetEdges = (edgeAdded ++ edges).distinct
		
		if (isEmpty(edges))
			return Graph(graph.vertices, newSetEdges)
		else
		{
			val updateFrontierGraph = updateFragmentId(minFragmentEdgeGraph).cache()
			val updateIdFragment = updateFragment(updateFrontierGraph).mapVertices{ case(vid, node) =>
				if(node.fragment.id < node.fragment.lastFragmentId) GHSNode(Fragment(node.fragment.id, node.fragment.id), AddedLink())
				else GHSNode(node.fragment, AddedLink()) }.cache()
			GHSrec(setMinFragmentId(updateIdFragment), newSetEdges)	
		}
	}
	
	def GHSMwst(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	{
		// set an emptyRDD to be the acc in the GHSrec, maybe it is better to use EmptyRDD
		val setEdges = graph.edges.filter(v => v != v)
		return GHSrec(setMinFragmentId(graph), setEdges)
	}	
}



