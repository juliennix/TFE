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
	
	class EdgeAttr() extends java.io.Serializable
	{
		var srcState = "BASIC"
		var dstState = "BASIC"
		var weight : Double = 0
		var function = ""
		var args :List[Float] = List()
	}

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
	def GHSGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Node, Double] = 
	{
        val nbNodes = samples.count.toInt
        val vertices: RDD[(VertexId, Node)] = samples.map {x =>
			(x.label.toLong, new Node(x.features.toArray))}
		val label = samples.map(x =>
			x.label)
		val cartesianLabel = label.cartesian(label).filter{case (label1, label2) => label1 < label2}
		val edges = cartesianLabel.map{ x =>
			Edge(x._1.toLong, x._2.toLong, 0.toDouble)
		}
		val graph = Graph(vertices, edges)
		val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		return weigthGraph
	}
	
	val computeMutInfo = (triplet : EdgeTriplet[Node, Double]) =>
			- mutInfo(triplet.srcAttr.sample, triplet.dstAttr.sample)
			
	
	def vertexProg(vid: VertexId, attr: Node, message: EdgeAttr) = 
	{
		println(vid, message.weight)
		message.function match
		{
			case "INIT" =>
			{ 
				attr.state = "FOUND"
			}
			case "WAKEUP" => 
			{
				attr.function = "WAKEUP"				
			}
			
			case _ => None
			
		}
		attr
	}
	
	def sendMessage(edge: EdgeTriplet[Node, EdgeAttr]):  Iterator[(org.apache.spark.graphx.VertexId, EdgeAttr)] = 
	{
		//~ println(edge.attr.weight, edge.srcId, edge.dstId)
		edge.attr.function match
		{
			case "" => 
			{
				edge.attr.function = "WAKEUP"
				Iterator((edge.dstId, edge.attr))

			}
			
			case "WAKEUP" => 
			{
				edge.attr.function = "CONNECT"
				edge.attr.srcState = "BRANCH"
				edge.attr.args = List(edge.srcAttr.level)
				Iterator((edge.dstId, edge.attr))
			}

			
			case _ => Iterator.empty
			
		}
	}
	
	def mergeMessage(msg1: EdgeAttr, msg2: EdgeAttr) = 
	{
		if (msg1.weight < msg2.weight)
			msg2
		else
			msg1
	}		
		
	val initialMessage = new EdgeAttr()
	initialMessage.function = "INIT"
	def PregelMST(graph : Graph[Node, EdgeAttr]) : Graph[Node, EdgeAttr] = 
	{
		Pregel(graph, initialMessage, activeDirection = EdgeDirection.Either)(
		vprog = vertexProg,
		sendMsg = sendMessage,
		mergeMsg = mergeMessage)
	}

}
	
	
