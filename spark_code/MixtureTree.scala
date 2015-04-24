/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Create mixture of trees                                     //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag	

import graphicalLearning.DistributedGraph._
import graphicalLearning.Kruskal._
import graphicalLearning.Prims._
import graphicalLearning.Boruvka._
import graphicalLearning.GHS._
import graphicalLearning.MarkovTree._
import graphicalLearning.Inference._
import graphicalLearning.Resample._

object MixtureTree extends Serializable
{ 
	
	def createMixtureWithBootstrap[VD: ClassTag](sc : SparkContext, input : RDD[(Double, Array[Double])], numberOfTree : Int) : Array[Graph[MarkovNode, Double]] =
	{
		val mixtureTree = new Array[Graph[MarkovNode, Double]](numberOfTree)
		for (i <- 0 to numberOfTree - 1)
		{
			// 'll see what kind of seed to put here
			val bootstrapInput = bootstrapRDD(input, 1)
			//~ val messageGraph = GHSGraph(bootstrapInput, sc)
			//~ val GHSMwstGraph = GHSMwst(messageGraph)
			//~ val markovTree = markovTreeCreation(GHSMwstGraph)
			val graph = RDDFastGraph(bootstrapInput,sc)
			val kruskalGraph = kruskalEdgesAndVertices(graph)
			val markovTree = markovTreeCreation(kruskalGraph)
			mixtureTree(i) = learnParameters(markovTree, input)
		}
		return mixtureTree
	}
	
	def sumMapByKey(map1 : Map[Double, Probability], map2 : Map[Double, Probability]) : Map[Double, Probability] =
	{
		map1 ++ map2.map{ case (k,prob) => k -> (Probability(prob.value + map1.getOrElse(k,Probability(0.0)).value)) }
	}
	
	def divMapByKey(map1 : Map[Double, Probability], map2 : Map[Double, Probability]) : Map[Double, Probability] =
	{
		map1 ++ map2.map{ case (k,prob) => k -> (Probability(prob.value / map1.getOrElse(k,Probability(0.0)).value)) }
	}
	
	def mulMapByKey(map1 : Map[Double, Probability], map2 : Map[Double, Probability]) : Map[Double, Probability] =
	{
		map1 ++ map2.map{ case (k,prob) => k -> (Probability(prob.value * map1.getOrElse(k,Probability(0.0)).value)) }
	}
	
	def getInferedProbability(mixtureTree : Array[Graph[MarkovNode, Double]], evidence : EvidenceSet, numberOfTree : Int) : RDD[(VertexId, Map[Double, Probability])] =
	{
		val inferedMixture = mixtureTree.map(tree => inference(tree, evidence))
        val weightByTree = Array.fill(numberOfTree)(1D/numberOfTree)
        val beliefByTree = inferedMixture.map( tree => tree.vertices.map(e => (e._1, e._2.belief.map{ case (key, prob) => (key -> Probability(prob.value/numberOfTree))})))
        return beliefByTree.reduce((a,b) => a.join(b).map{ case(key, (map1, map2)) => (key, sumMapByKey(map1, map2))})
	}

}
