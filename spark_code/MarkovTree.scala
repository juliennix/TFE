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


object MarkovNode extends Serializable 
{
	def apply(): MarkovNode = MarkovNode(0, Map[JointEvent, Probability]())
}
case class MarkovNode(level: Double, cpt: Map[JointEvent, Probability] = Map()) extends Serializable

case class JointEvent(variable : Double, condition : Double) extends Serializable 

case class Probability(value: Double) extends Serializable


// Could create a bipartite graph by computin the different
// factors/probabilities in order to have a more general representation
// i.e. for Bayesian network. Instead, a tree structure is keeped as it
// give better performance
object MarkovTree extends Serializable 
{
	
	def sendMessage[ED: ClassTag](triplet: EdgeTriplet[MarkovNode, ED]):  Iterator[(org.apache.spark.graphx.VertexId, Double)] = 
	{ 
		if (triplet.srcAttr.level != Double.PositiveInfinity && triplet.dstAttr.level == Double.PositiveInfinity )
			Iterator((triplet.dstId, triplet.srcAttr.level + 1D))
		else if (triplet.dstAttr.level != Double.PositiveInfinity && triplet.srcAttr.level == Double.PositiveInfinity)
			Iterator((triplet.srcId, triplet.dstAttr.level + 1D))
		else
			Iterator.empty
	}
	
	def markovTreeCreation[VD: ClassTag, ED: ClassTag](graph : Graph[VD, ED]) : Graph[MarkovNode, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) MarkovNode(0D) else MarkovNode(Double.PositiveInfinity))
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		MarkovNode(math.min(attr.level, msg)), sendMsg = sendMessage, (messag1, message2) => math.min(messag1, message2))
		val rightEdges = bfs.triplets.map{ triplet => {
			if (triplet.srcAttr.level < triplet.dstAttr.level) Edge(triplet.srcId, triplet.dstId, 0D)
			else Edge(triplet.dstId, triplet.srcId, 0D)
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
	
    def conditionalProb(variable : Array[Double], condition : Array[Double]): Map[JointEvent, Probability] =
    {
		val length = variable.length
		val pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/length)
		val conjMap = mapFunction(length, variable, condition)
		return conjMap.map
		{
			case ((variable, condition), value) =>
			{
				val norm = (value.toDouble / length)
				(JointEvent(variable, condition), Probability((norm / pY(condition))))
			}
		}
	}  	
	
	def weightedMapFunction(length : Int, variable : Array[(Double, Probability)], condition : Array[(Double, Probability)], i :Int = 0, conjMap : Map[(Double,Double), Double] = Map()) : Map[(Double,Double), Double] = 
	{
		if (i == length) return conjMap
		else
		{
			if (conjMap.contains((variable(i)._1, condition(i)._1))){
				val newConjMap = conjMap.updated((variable(i)._1, condition(i)._1), conjMap((variable(i)._1, condition(i)._1)) + variable(i)._2.value)
				weightedMapFunction(length, variable, condition, i+1, newConjMap)
			}
			else{
				val newConjMap = conjMap + ((variable(i)._1, condition(i)._1) -> variable(i)._2.value)
				weightedMapFunction(length, variable, condition, i+1, newConjMap)
			}
		}	
	}
	
    def weightedConditionalProb(variable : Array[(Double, Probability)], condition : Array[(Double, Probability)]): Map[JointEvent, Probability] =
    {
		val length = variable.length
		val pYFreq = condition.groupBy(x=>x._1)
		val pYWeight = pYFreq.map{case (key, arr) => arr.reduce((a,b) => (a._1,Probability( a._2.value + b._2.value)))}
		val totWeight = pYWeight.reduce((a,b) => (a._1,Probability(a._2.value + b._2.value)))._2.value
		val pY = pYWeight.map{ case(key, prob) => key -> prob.value/totWeight	}

		return weightedMapFunction(length, variable, condition).map{
			case ((variable, condition), value) =>
			{
				val norm = (value.toDouble / totWeight)
				(JointEvent(variable, condition), Probability((norm / pY(condition))))
			}}
	}  	
			
	// Compute the parameters for the root
	// Notice that it return also a Map variable, condition) -> prob as we have to have the 
	// same type than for the node, thus, here, the Map corresponds to (variable, variable) -> prob
	def margProb(l : Array[Double]): Map[JointEvent, Probability] = 
    {
        val length = l.length
        val freq = l.groupBy(x=>JointEvent(x, x)).mapValues(v => Probability(v.size.toDouble/length))
        return freq.map{ x => x}
    }  
	def weightedMargProb(l : Array[(Double, Probability)]): Map[JointEvent, Probability] = 
    {
		val freq = l.groupBy(x=>JointEvent(x._1, x._1))
        val weight = freq.map{case (key, arr) => (key, arr.reduce((a,b) => (a._1,Probability( a._2.value + b._2.value))))}
        val totWeight = weight.reduce((a,b) => (a._1, (a._2._1, Probability(a._2._2.value + b._2._2.value))))._2._2.value
		return weight.map{ case(key, (v, prob)) => key -> Probability(prob.value/totWeight)}
    }  
    

	// Learn Maximum Likelyhood Estimation
	def learnParameters(markovTree : Graph[MarkovNode, Double], samples :  RDD[(Double, Array[Double])]) : Graph[MarkovNode, Double] = 
	{
		val cart = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 != key2}
		val keyValue = cart.map{ case ((key1, val1), (key2, val2)) => ((key1.toLong, key2.toLong), (val1, val2))}
		val relation = markovTree.triplets.map(t => ((t.srcId, t.dstId), t.dstAttr.level))
		val joined = relation.join(keyValue)
		val condProbChildren = joined.map{ case ((key1, key2),(levelChild,(array1, array2))) => (key2, MarkovNode(levelChild, conditionalProb(array1, array2)))}
		
		// Retrieve the root without retrieving data on the local drive but use a subtractByKey which is a more complex operation
		val labels = markovTree.vertices.filter{ case (vid, markovNode) => markovNode.level != 0.0}.map(x => (x._1.toDouble, 0D))
		val root = samples.subtractByKey(labels).map{ case (label, sample) => (label.toLong, MarkovNode(0D, margProb(sample)))}
		
		// Retrieve the root by retrieving the rootlabel on the local drive
		//~ val rootLabel = markovTree.vertices.filter{ case (vid, markovNode) => markovNode.level == 0.0}.keys.first
		//~ val root = samples.filter{ case (label, sample) => label == rootLabel}.map{ case (label, sample) => (label.toLong, MarkovNode(0D, margProb(sample)))}
		
		val vertices = condProbChildren union root
		return Graph(vertices, markovTree.edges)
	}
	
	def learnWeightedParameters(markovTree : Graph[MarkovNode, Double], samples :  RDD[(Double, Array[(Double, Probability)])]) : Graph[MarkovNode, Double] = 
	{
		val cart = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 != key2}
		val keyValue = cart.map{ case ((key1, val1), (key2, val2)) => ((key1.toLong, key2.toLong), (val1, val2))}
		val relation = markovTree.triplets.map(t => ((t.srcId, t.dstId), t.dstAttr.level))
		val joined = relation.join(keyValue)
		val condProbChildren = joined.map{ case ((key1, key2),(levelChild,(array1, array2))) => (key2, MarkovNode(levelChild, weightedConditionalProb(array1, array2)))}
		
		// Retrieve the root without retrieving data on the local drive but use a subtractByKey which is a more complex operation
		val labels = markovTree.vertices.filter{ case (vid, markovNode) => markovNode.level != 0.0}.map(x => (x._1.toDouble, 0D))
		val root = samples.subtractByKey(labels).map{ case (label, sample) => (label.toLong, MarkovNode(0D, weightedMargProb(sample)))}
		
		// Retrieve the root by retrieving the rootlabel on the local drive
		//~ val rootLabel = markovTree.vertices.filter{ case (vid, markovNode) => markovNode.level == 0.0}.keys.first
		//~ val root = samples.filter{ case (label, sample) => label == rootLabel}.map{ case (label, sample) => (label.toLong, MarkovNode(0D, margProb(sample)))}
		
		val vertices = condProbChildren union root
		return Graph(vertices, markovTree.edges)
	}
}
