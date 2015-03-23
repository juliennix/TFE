/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Manages the creation and techniques on distr. graph         //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import graphicalLearning.MutualInfo._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


object GHS
{
	def GHSGraph(samples :  RDD[(Double, Array[Double])], sc : SparkContext) :  Graph[((Long, Long), (Long,Long)), Double] = 
	{			
		val mutualInfo = mutInfoRDD(samples)
        val vertices: RDD[(VertexId, ((Long,Long), (Long, Long)))] = samples.map { case(k,v) =>
			(k.toLong, ((k.toLong, k.toLong), (-1.toLong, -1.toLong)))}
		val edges = mutualInfo.map{ case ((key1, key2), weight) => Edge(key1.toLong, key2.toLong, - weight)}
		val graph = Graph(vertices, edges)
		return graph
	}
	
	
	//~ def GHSGraph(samples :  RDD[(Double, Array[Double])], sc : SparkContext) :  Graph[((Long, Long), List[(Long,Long)]), EdgeAttr] = 
	//~ {			
		//~ val mutualInfo = mutInfoRDD(samples)
        //~ val vertices: RDD[(VertexId, ((Long,Long), List[(Long,Long)]))] = samples.map { case(k,v) =>
			//~ (k.toLong, ((k.toLong, k.toLong), List[(Long,Long)]()))}
		//~ val edges = mutualInfo.map{ case ((key1, key2), weight) => Edge(key1.toLong, key2.toLong, new EdgeAttr("BASIC", - weight, -1.toLong))}
		//~ val graph = Graph(vertices, edges)
		//~ return graph
	//~ }


	def vertexProg(vid: VertexId, attr: ((Long, Long), (Long, Long)), message: (Long, Long, Double)) = 
	{
		if (message._1 == -1)
			attr
		else
		{
			((attr._1._1, message._2),  attr._2)
		}
	}
	
	def sendMessage(triplet: EdgeTriplet[((Long,Long), (Long,Long)), Double]):  Iterator[(org.apache.spark.graphx.VertexId, (Long, Long, Double))] = 
	{
		if (triplet.srcAttr._1._1 != triplet.dstAttr._1._1){
			Iterator((triplet.srcId, (triplet.dstAttr._1._1, triplet.dstId, triplet.attr)), (triplet.dstId, (triplet.srcAttr._1._1 ,triplet.srcId, triplet.attr)))
		}
		else
			Iterator.empty
	}
	
	def mergeMessage(msg1: (Long, Long, Double), msg2: (Long, Long, Double)) = 
	{
		if (msg1._3 < msg2._3)
			msg1
		else
			msg2
	}		
		
	def PregelMST(graph : Graph[((Long, Long), (Long,Long)), Double]) : Graph[((Long, Long), (Long,Long)), Double] = 
	{
		val initialMessage = (-1.toLong, -1.toLong, 0.toDouble)
		val numIter = 1
		Pregel(graph, initialMessage,  numIter, EdgeDirection.Both)(
		vprog = vertexProg,
		sendMsg = sendMessage,
		mergeMsg = mergeMessage)
	}
	
	def isEmpty[T](rdd : RDD[T]) = {
		rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_) 
	}
	
	def mstRec(graph : Graph[((Long, Long), (Long,Long)), Double], finalE : RDD[Edge[Double]], iter : Int = 0) : (Graph[((Long, Long), (Long,Long)), Double]) = 
	{
		
		val nodes = graph.aggregateMessages[((Long, Long), (Long, Long))](
			triplet => {
				if (triplet.dstId == triplet.srcAttr._1._2.toLong)
				{
					if (triplet.srcAttr._1._1 > triplet.dstAttr._1._1)
						triplet.sendToSrc((triplet.dstAttr._1._1, triplet.dstAttr._1._1), (triplet.dstId, triplet.srcId))
					else if (triplet.srcAttr._1._1 < triplet.dstAttr._1._1)
						triplet.sendToSrc((triplet.srcAttr._1._1, triplet.srcAttr._1._1), (triplet.dstId, triplet.srcId))

				}
				if ( triplet.srcId == triplet.dstAttr._1._2.toLong) 
				{
					if (triplet.dstAttr._1._1 > triplet.srcAttr._1._1)
						triplet.sendToDst((triplet.srcAttr._1._1, triplet.srcAttr._1._1), (triplet.srcId, triplet.dstId))
					else if (triplet.dstAttr._1._1 < triplet.srcAttr._1._1)
						triplet.sendToDst((triplet.dstAttr._1._1, triplet.dstAttr._1._1), (triplet.srcId, triplet.dstId))
				}
			},
		(a,b) => a)
		if (isEmpty(nodes) && iter == 0){
			return Graph(graph.vertices, finalE)
		} 
		if (isEmpty(nodes) && iter != 0) {
			val edges = nodes.filter{ case (vid, ((vid2, id), (src, dst))) => src < dst}.map{ case (vid, ((vid2, id), (src, dst))) => Edge(src, dst, 0.0)}
			mstRec(PregelMST(graph), (finalE ++ edges).distinct)}
		else{
			val edges = nodes.filter{ case (vid, ((vid2, id), (src, dst))) => src < dst}.map{ case (vid, ((vid2, id), (src, dst))) => Edge(src, dst, 0.0)}
			val newNodes = graph.vertices.subtractByKey(nodes).union(nodes)
			val newGraph = (Graph(newNodes, graph.edges))
			mstRec(newGraph, (finalE ++ edges).distinct, iter + 1)
		}
	}
	
	def GHSmst(graph : Graph[((Long, Long), (Long,Long)), Double]) : Graph[((Long, Long), (Long,Long)), Double] = 
	{
		val pregelGraph = PregelMST(graph)
		val setEdges = pregelGraph.edges.filter(v => v != v)
		return mstRec(pregelGraph, setEdges)
		
	}
	
	class Node(samples : Array[Double]) extends java.io.Serializable
	{
		var sample = samples
		var level = -1
		var Find_count = 0
		var fragm = None
		var state = "SLEEPING"
		var function = ""
		var best_edge = None
		var best_wt = 0
		var test_edge = None
		var phase = None
		var state_edge = "BASIC"
		var parentList = List[Long]()
		var id : Long = 0
		
	}
	
	//~ class EdgeAttr() extends java.io.Serializable
	//~ {
		//~ var srcState = "BASIC"
		//~ var dstState = "BASIC"
		//~ var weight : Double = 0
		//~ var function = ""
		//~ var args :List[Float] = List()
	//~ }
	class EdgeAttr(branched : String,weightE : Double, attribute : Long) extends java.io.Serializable
	{
		var state = branched
		var attr : Long = attribute
		var weight : Double = weightE
	}
	// other ways to compute different graph with matched older version of the GHS algortihm
	//~ def GHSGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Node, EdgeAttr] = 
	//~ {
        //~ val nbNodes = samples.count.toInt
        //~ val vertices: RDD[(VertexId, Node)] = samples.map {x =>
			//~ (x.label.toLong, new Node(x.features.toArray))}
		//~ var label = samples.map(x =>
			//~ x.label)
		//~ val cartesianLabel = label.cartesian(label).filter(x => x._1 != x._2)
		//~ val edges = cartesianLabel.map{ x =>
			//~ Edge(x._1.toLong, x._2.toLong, new EdgeAttr ())
		//~ }
		//~ var graph = Graph(vertices, edges)
		//~ graph = graph.mapTriplets
		//~ {
			//~ triplet => 
			//~ {
				//~ triplet.attr.weight = computeMutInfo(triplet)
				//~ triplet.attr
			//~ }
		//~ }
		//~ return graph
	//~ }
	//~ def NodeGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Node, Double] = 
	//~ {
        //~ val nbNodes = samples.count.toInt
        //~ val vertices: RDD[(VertexId, Node)] = samples.map {x =>
			//~ (x.label.toLong, new Node(x.features.toArray))}
		//~ val label = samples.map(x =>
			//~ x.label)
		//~ val cartesianLabel = label.cartesian(label).filter{case (label1, label2) => label1 < label2}
		//~ val edges = cartesianLabel.map{ x =>
			//~ Edge(x._1.toLong, x._2.toLong, 0.toDouble)
		//~ }
		//~ val graph = Graph(vertices, edges)
		//~ val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		//~ return weigthGraph
	//~ }
	//~ 
	//~ val computeMutInfo = (triplet : EdgeTriplet[Node, Double]) =>
			//~ - mutInfo(triplet.srcAttr.sample, triplet.dstAttr.sample)
			
	//~ def vertexProg(vid: VertexId, attr: Node, message: EdgeAttr) = 
	//~ {
		//~ println(vid, message.weight)
		//~ message.function match
		//~ {
			//~ case "INIT" =>
			//~ { 
				//~ attr.state = "FOUND"
			//~ }
			//~ case "WAKEUP" => 
			//~ {
				//~ attr.function = "WAKEUP"				
			//~ }
			//~ 
			//~ case _ => None
			//~ 
		//~ }
		//~ attr
	//~ }
	//~ 
	//~ def sendMessage(edge: EdgeTriplet[Node, EdgeAttr]):  Iterator[(org.apache.spark.graphx.VertexId, EdgeAttr)] = 
	//~ {
		//~ println(edge.attr.weight, edge.srcId, edge.dstId)
		//~ edge.attr.function match
		//~ {
			//~ case "" => 
			//~ {
				//~ edge.attr.function = "WAKEUP"
				//~ Iterator((edge.dstId, edge.attr))
//~ 
			//~ }
			//~ 
			//~ case "WAKEUP" => 
			//~ {
				//~ edge.attr.function = "CONNECT"
				//~ edge.attr.srcState = "BRANCH"
				//~ edge.attr.args = List(edge.srcAttr.level)
				//~ Iterator((edge.dstId, edge.attr))
			//~ }
//~ 
			//~ 
			//~ case _ => Iterator.empty
			//~ 
		//~ }
	//~ }
	//~ 
	//~ def mergeMessage(msg1: EdgeAttr, msg2: EdgeAttr) = 
	//~ {
		//~ if (msg1.weight < msg2.weight)
			//~ msg2
		//~ else
			//~ msg1
	//~ }		
		//~ 
	//~ val initialMessage = new EdgeAttr()
	//~ initialMessage.function = "INIT"
	//~ def PregelMST(graph : Graph[Node, EdgeAttr]) : Graph[Node, EdgeAttr] = 
	//~ {
		//~ Pregel(graph, initialMessage, activeDirection = EdgeDirection.Either)(
		//~ vprog = vertexProg,
		//~ sendMsg = sendMessage,
		//~ mergeMsg = mergeMessage)
	//~ }
}
	
	
