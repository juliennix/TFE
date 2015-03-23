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
	
	//~ def send(triplet: EdgeTriplet[Double, Double]):  Iterator[(org.apache.spark.graphx.VertexId, (Double, RDD[(Double, Array[Double]))] = 
	//~ { 
		//~ Iterator((tiplet.dstId, ( triplet.srcId, 
	//~ }
	//~ 
	//~ setUpProbNode(vid: VertexId, attr: Map[Double, Double], message: (Long, RDD[(Double, Array[Double]))) = 
	//~ {
		//~ val father = message._1
		//~ val samples = message._2
		//~ if (father == 0.toDouble)
			//~ samples.filter(x => x._1 == vid).mapValues{a => a.groupBy(x => x).map( a => (a._1, a._2.length.toDouble/240))}.first._2
		//~ else
			//~ 
	def mapFunction(length : Int, variable : Array[Double], condition : Array[Double], i :Int = 0, conjMap : Map[(Double,Double), Int] = Map()) : Map[(Double,Double), Int] = 
	{
		if (i == length) return conjMap
		else
		{
			if (conjMap.contains((variable(i), condition(i)))){
				val newConjMap = conjMap.updated((variable(i), condition(i)), conjMap((variable(i), condition(i))) + 1)
				mapFunction(length, variable, condition, i+1, newConjMap)
			}
			else{
				val newConjMap = conjMap + ((variable(i), condition(i)) -> 1)
				mapFunction(length, variable, condition, i+1, newConjMap)
			}
		}	
	}
    def conditionalProb(variable : Array[Double], condition : Array[Double]): Map[(Double, Double), Double] =
    {
		val length = variable.length
		val pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/length)
		val conjMap = mapFunction(length, variable, condition)
		return conjMap.map
		{
			case (key, value) =>
			{
				val norm = (value.toDouble / length)
				(key, (norm / pY(key._2)))
			}
		}
	}  			

	def margProb(l : Array[Double]): Map[(Double, Double), Double] = 
    {
        val length = l.length
        return l.groupBy(x=>(x, x)).mapValues(_.size.toDouble/length)
    }  

	
	def setUpMarkovTree(markovTree : Graph[Double, Double], samples :  RDD[(Double, Array[Double])]) : Graph[Map[(Double, Double), Double], Double] = 
	{
		val cart = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 < key2}
		val keyValue = cart.map{ case ((key1, val1), (key2, val2)) => ((key1.toLong, key2.toLong), (val1, val2))}
		val relation = markovTree.triplets.map(t => ((t.srcId, t.dstId), 0))
		val joined = relation.join(keyValue)
		val c = joined.map{ case ((key1, key2),(val1,(array1, array2))) => (key1, conditionalProb(array1, array2))}
		val rootLabel = markovTree.vertices.filter{ case (vid, level) => level == 0.0}.keys
		val root = samples.filter{ case (label, sample) => label == rootLabel}.map{ case (label, sample) => (label.toLong, margProb(sample))}
		val vertices = c union root
		return Graph(vertices, markovTree.edges)
	}	
	
	//~ def beliefPropagation()
	
	
	
	
}
