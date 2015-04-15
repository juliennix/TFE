/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Create and set up markov tree                               //
///////////////////////////////////////////////////////////////// 	
	
	
package graphicalLearning	
	
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._	
import scala.reflect.ClassTag

	
// Could create a bipartite graph by computin the different
// factors/probabilities in order to have a more general representation
// i.e. for Bayesian network. Instead, a tree structure is keeped as it
// give better performance
object MarkovTree extends Serializable 
{
	
	def sendMessage[ED: ClassTag](triplet: EdgeTriplet[Double, ED]):  Iterator[(org.apache.spark.graphx.VertexId, Double)] = 
	{ 
		if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity )
			Iterator((triplet.dstId, triplet.srcAttr + 1D))
		else if (triplet.dstAttr != Double.PositiveInfinity && triplet.srcAttr == Double.PositiveInfinity)
			Iterator((triplet.srcId, triplet.dstAttr + 1D))
		else
			Iterator.empty
	}
	
	def markovTreeCreation[VD: ClassTag, ED: ClassTag](graph : Graph[VD, ED]) : Graph[Double, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		math.min(attr, msg), sendMsg = sendMessage, (a,b) => math.min(a,b))
		val rightEdges = bfs.triplets.map{ t => {
			if (t.srcAttr < t.dstAttr) Edge(t.srcId, t.dstId, 0D)
			else Edge(t.dstId, t.srcId, 0D)
			}
		}
		val markovTree = Graph(bfs.vertices, rightEdges)
		return markovTree
	}	

	// Compute the parameters for the intern nodes and the leaves
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
			
	// Compute the parameters for the root
	// Notice that it return also a Map variable, condition) -> prob as we have to have the 
	// same type than for the node, thus, here, the Map corresponds to (variable, variable) -> prob
	def margProb(l : Array[Double]): Map[(Double, Double), Double] = 
    {
        val length = l.length
        val freq = l.groupBy(x=>(x, x)).mapValues(_.size.toDouble/length)
        return freq.map{ x => x}
    }  

	// Learn Maximum Likelyhood Estimation
	def learnParameters(markovTree : Graph[Double, Double], samples :  RDD[(Double, Array[Double])]) : Graph[(Map[(Double, Double), Double], Double), Double] = 
	{
		val cart = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 != key2}
		val keyValue = cart.map{ case ((key1, val1), (key2, val2)) => ((key1.toLong, key2.toLong), (val1, val2))}
		val relation = markovTree.triplets.map(t => ((t.srcId, t.dstId), 0))
		val joined = relation.join(keyValue)
		val c = joined.map{ case ((key1, key2),(val1,(array1, array2))) => (key2, conditionalProb(array1, array2))}
		
		// Retrieve the root without retrieving data on the local drive but use a subtractByKey which is a more complex operation
		val labels = markovTree.vertices.filter{ case (vid, level) => level != 0.0}.map(x => (x._1.toDouble, 0))
		val root = samples.subtractByKey(labels).map{ case (label, sample) => (label.toLong, margProb(sample))}
		
		// Retrieve the root by retrieving the rootlabel on the local drive
		//~ val rootLabel = markovTree.vertices.filter{ case (vid, level) => level == 0.0}.keys.first
		//~ val root = samples.filter{ case (label, sample) => label == rootLabel}.map{ case (label, sample) => (label.toLong, margProb(sample))}
		
		val vertices = c union root join markovTree.vertices
		return Graph(vertices, markovTree.edges)
	}
}
