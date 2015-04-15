/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Create mixture of trees                                     //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.ClassTag	

import graphicalLearning.DistributedGraph._
import graphicalLearning.Prims._
import graphicalLearning.Boruvka._
import graphicalLearning.GHS._
import graphicalLearning.MarkovTreeProposition._
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
			val graph4 = RDDFastGraph(bootstrapInput,sc)
			val primGraph = PrimsAlgo(graph4)
			val markovTree = markovTreeCreation(primGraph)
			mixtureTree(i) = learnParameters(markovTree, input)
		}
		return mixtureTree
	}
}
