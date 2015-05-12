//~ /////////////////////////////////////////////////////////////////
//~ // Author : Nix Julien                                         //        
//~ // For the University of Liège                                 //     
//~ // Compute the mwst thanks to message passing algorithm        //
//~ ///////////////////////////////////////////////////////////////// 
//~ 
//~ package graphicalLearning
//~ 
//~ import org.apache.spark.graphx.EdgeDirection
//~ import org.apache.spark.graphx.Graph
//~ import org.apache.spark.graphx.EdgeTriplet
//~ import org.apache.spark.graphx.Edge
//~ import org.apache.spark.graphx.VertexId
//~ import org.apache.spark.rdd.RDD
//~ import org.apache.spark.SparkContext._
//~ 
//~ 
//~ object Fragment extends Serializable
//~ {
	//~ def apply():Fragment = Fragment(-1L, -1L, 0D)
//~ }
//~ 
//~ object AddedLink extends Serializable 
//~ {
	//~ def apply():AddedLink = AddedLink(-1L, -1L)
//~ }
//~ 
//~ case class Fragment(id: VertexId, tempId: VertexId = 0L, minWeight : Double = 0D) extends Serializable
//~ 
//~ case class AddedLink(from: VertexId, to: VertexId) extends Serializable
//~ 
//~ case class GHSEdge(weight: Double) extends Serializable
//~ 
//~ object GHSNode extends Serializable 
//~ {
	//~ def apply(): GHSNode = GHSNode(Fragment(), AddedLink())
//~ }
//~ case class GHSNode(fragment: Fragment, addedLink: AddedLink) extends Serializable
//~ 
//~ case class GHSMessage(node: GHSNode, edgeValue: GHSEdge) extends Serializable 
//~ {
	//~ var isUnset: Boolean = node.fragment.id == -1L
//~ }
//~ 
//~ object GHS extends Serializable
//~ {
	//~ 
	//~ /////////////////////////////////////////////First phase////////////////////////////////////////////
	//~ 
	//~ // => compute the minimum edges from a fragment to another keeping the corresponding fragment id
	//~ def vertexProg(vid: VertexId, attr: GHSNode, message: GHSMessage) = message.isUnset match 
	//~ {
		//~ case true => attr
		//~ case false => GHSNode(Fragment(attr.fragment.id, message.node.fragment.id, message.edgeValue.weight), message.node.addedLink)
	//~ }
	//~ 
	//~ def sendMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, GHSMessage)] = 
	//~ {
		//~ if (triplet.srcAttr.fragment.id != triplet.dstAttr.fragment.id) 
		//~ {
			//~ val updatedSrcFragment = GHSMessage(GHSNode(Fragment(triplet.dstAttr.fragment.id), AddedLink(triplet.srcId, triplet.dstId)), triplet.attr)
			//~ val updatedDstFragment  = GHSMessage(GHSNode(Fragment(triplet.srcAttr.fragment.id), AddedLink(triplet.srcId, triplet.dstId)), triplet.attr)
			//~ Iterator((triplet.srcId, updatedSrcFragment), (triplet.dstId, updatedDstFragment))
		//~ }
		//~ else
			//~ Iterator.empty
	//~ }
	//~ 
	//~ def mergeMessage(msg1: GHSMessage, msg2: GHSMessage) =
	//~ {
		//~ if (msg1.edgeValue.weight < msg2.edgeValue.weight) msg1
		//~ else if	(msg1.edgeValue.weight == msg2.edgeValue.weight) 
		//~ {
			//~ if (msg1.node.addedLink.from < msg2.node.addedLink.from) msg1
			//~ else if (msg1.node.addedLink.from == msg2.node.addedLink.from)
			//~ {
				//~ if (msg1.node.addedLink.to < msg2.node.addedLink.to) msg1
				//~ else msg2
			//~ }
			//~ else msg2
		//~ }
		//~ else msg2
	//~ }	
		//~ 
	//~ def setMinFragmentId(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	//~ {
		//~ val initialMessage = GHSMessage(GHSNode(), GHSEdge(0D))
		//~ val numIter = 1
	//~ 
		//~ graph.pregel(initialMessage, numIter, EdgeDirection.Both)(
		//~ vprog = vertexProg,
		//~ sendMsg = sendMessage,
		//~ mergeMsg = mergeMessage)
	//~ }
	//~ 
	//~ 
	//~ /////////////////////////////////////////////Second phase////////////////////////////////////////////
	//~ 
	//~ // => compute the minimum edges in a fragment in order to link fragment with the minimum edge in the fragment
	//~ def fragmentProg(vid: VertexId, attr: GHSNode, message: GHSMessage) = message.isUnset match 
	//~ {
		//~ case true => attr
		//~ case false =>
		//~ {
			//~ var change = false
			//~ if (message.edgeValue.weight < attr.fragment.minWeight)
				//~ change = true
			//~ else if (message.edgeValue.weight ==  attr.fragment.minWeight)
			//~ {
				//~ if(message.node.fragment.id < vid)
					//~ change = true
			//~ }
			//~ if(attr.fragment.id > message.node.fragment.tempId)
				//~ if(change) GHSNode(Fragment(message.node.fragment.tempId), AddedLink())
				//~ else GHSNode(Fragment(message.node.fragment.tempId), attr.addedLink)
			//~ else
				//~ if(change) GHSNode(Fragment(attr.fragment.id), AddedLink())
				//~ else GHSNode(Fragment(attr.fragment.id), attr.addedLink)
		//~ } 
	//~ }
//~ 
	//~ def sendInFragmentMessage(triplet: EdgeTriplet[GHSNode, GHSEdge]): Iterator[(VertexId, GHSMessage)] = 
	//~ {
		//~ if (triplet.srcAttr.fragment.id == triplet.dstAttr.fragment.id) 
		//~ {
				//~ val updatedSrcFragment = GHSMessage(GHSNode(Fragment(triplet.dstId, triplet.dstAttr.fragment.tempId), AddedLink()), GHSEdge(triplet.dstAttr.fragment.minWeight))
				//~ val updatedDstFragment = GHSMessage(GHSNode(Fragment(triplet.srcId, triplet.srcAttr.fragment.tempId), AddedLink()), GHSEdge(triplet.srcAttr.fragment.minWeight))
				//~ Iterator((triplet.srcId, updatedSrcFragment), (triplet.dstId, updatedDstFragment))				
		//~ }
		//~ else
			//~ Iterator.empty
	//~ }
	//~ 
	//~ def computeMinInFragment(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	//~ {
		//~ val initialMessage = GHSMessage(GHSNode(), GHSEdge(0D))
		//~ val numIter = 1
		//~ 
		//~ graph.pregel(initialMessage, numIter, EdgeDirection.Both)(
		//~ vprog = fragmentProg,
		//~ sendMsg = sendInFragmentMessage,
		//~ mergeMsg = mergeMessage)
	//~ }
	//~ 
	//~ /////////////////////////////////////////////GHS recursive algorithm////////////////////////////////////////////
//~ 
	//~ def isEmpty[T](rdd : RDD[T]) = 
	//~ {
		//~ rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_) 
	//~ }
	//~ 
	//~ def GHSrec(graph : Graph[GHSNode, GHSEdge], edgeAdded : RDD[Edge[GHSEdge]]) : Graph[GHSNode, GHSEdge] = 
	//~ {
		//~ // update the fragment Id and check which is the best edges in a fragment
		//~ val minFragmentEdgeGraph = computeMinInFragment(graph).cache()
		//~ // compute the edges to be kept
		//~ val edges = minFragmentEdgeGraph.vertices.map{ case(vid,node) => Edge(node.addedLink.from, node.addedLink.to, GHSEdge(0D))}.filter{link => link.srcId != -1L}
		//~ val newSetEdges = (edgeAdded ++ edges).distinct
		//~ if (isEmpty(edges))
			//~ return Graph(graph.vertices, newSetEdges)
		//~ else
		//~ {
			//~ val newGraph = minFragmentEdgeGraph.mapVertices{ case(vid, node) =>
				//~ if(node.fragment.id < node.fragment.tempId) GHSNode(Fragment(node.fragment.id, node.fragment.id), AddedLink())
				//~ else GHSNode(Fragment(node.fragment.tempId, node.fragment.tempId), AddedLink()) }
			//~ GHSrec(setMinFragmentId(newGraph), newSetEdges)	
		//~ }
	//~ }
//~ 
	//~ def GHSMwst(graph : Graph[GHSNode, GHSEdge]) : Graph[GHSNode, GHSEdge] = 
	//~ {
		//~ // set an emptyRDD to be the acc in the GHSrec, maybe it is better to use EmptyRDD
		//~ val setEdges = graph.edges.filter(v => v != v)
		//~ return GHSrec(setMinFragmentId(graph), setEdges)
	//~ }	
//~ }
//~ 
