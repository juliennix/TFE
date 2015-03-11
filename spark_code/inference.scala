package graphicalLearning

import graphicalLearning.GHS._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


// create a bi partite graph, use a BN and fix the direction of the edges in order to create a 
// markov tree. then fill the partite graph trhoug the markov tree and compute the different
// factor/conditional probability 
object Inference
{
	def sendMessage(triplet: EdgeTriplet[Double, Double]):  Iterator[(org.apache.spark.graphx.VertexId, Double)] = 
	{ 
		if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity )
			Iterator((triplet.dstId, triplet.srcAttr + 1))
		else if (triplet.dstAttr != Double.PositiveInfinity && triplet.srcAttr == Double.PositiveInfinity)
			Iterator((triplet.srcId, triplet.dstAttr + 1))
		else
			Iterator.empty
	}
	def orientGraph(graph : Graph[Node, Double]) : Graph[Node, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		math.min(attr, msg), sendMsg = sendMessage, (a,b) => math.min(a,b))
		val rightEdges = bfs.triplets.map{ t => {
			if (t.srcAttr < t.dstAttr) Edge(t.srcId, t.dstId, t.attr)
			else Edge(t.dstId, t.srcId, t.attr)
			}
		}
		val markovTree = Graph(graph.vertices, rightEdges)
		markovTree.edges.foreach(println)
		return markovTree
	}	
	def orientedGraph(graph : Graph[Double, Double]) : Graph[Double, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		math.min(attr, msg), sendMsg = sendMessage, (a,b) => math.min(a,b))
		val rightEdges = bfs.triplets.map{ t => {
			if (t.srcAttr < t.dstAttr) Edge(t.srcId, t.dstId, t.attr)
			else Edge(t.dstId, t.srcId, t.attr)
			}
		}
		val markovTree = Graph(graph.vertices, rightEdges)
		markovTree.edges.foreach(println)
		return markovTree
	}	
}
