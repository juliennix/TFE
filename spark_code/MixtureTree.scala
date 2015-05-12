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
	// create a mixture of tree and store them in an array
	def createMixtureWithBootstrap[VD: ClassTag](sc : SparkContext, input : RDD[(Double, Array[Double])], numberOfTree : Int) : Array[Graph[MarkovNode, Double]] =
	{
		val emptyMixture = new Array[Graph[MarkovNode, Double]](numberOfTree)
		val mixtureTree = emptyMixture.map( e=> 
		{
			// 'll see what kind of seed to put here
			val bootstrapInput = bootstrapRDD(input, 1)
			val messageGraph = GHSGraph(bootstrapInput, sc)
			val GHSMwstGraph = GHSMwst(messageGraph)
			val markovTree = markovTreeCreation(GHSMwstGraph)
			//~ val graph = RDDFastGraph(bootstrapInput,sc)
			//~ val kruskalGraph = kruskalEdgesAndVertices(graph)
			//~ val markovTree = markovTreeCreation(kruskalGraph)
			learnParameters(markovTree, input)
		})
		return mixtureTree
	}
	
	def createMixtureWithBootstrapBayes[VD: ClassTag](sc : SparkContext, input : RDD[(Double, Array[Double])], numberOfTree : Int) : Array[Graph[MarkovNode, Double]] =
	{
		val emptyMixture = new Array[Graph[MarkovNode, Double]](numberOfTree)
		val mixtureTree = emptyMixture.map( e=> 
		{
			// 'll see what kind of seed to put here
			val bootstrapInput = bootstrapRDD(input, 1)
			val messageGraph = GHSGraph(bootstrapInput, sc)
			val GHSMwstGraph = GHSMwst(messageGraph)
			val markovTree = markovTreeCreation(GHSMwstGraph)
			//~ val graph = RDDFastGraph(bootstrapInput,sc)
			//~ val kruskalGraph = kruskalEdgesAndVertices(graph)
			//~ val markovTree = markovTreeCreation(kruskalGraph)
			learnBayesParameters(markovTree, input)
		})
		return mixtureTree
	}
	
	def recEM(sc: SparkContext, train : RDD[(Double, Array[Double])], mixtureTree : Array[Graph[SecondPhaseNode, Double]], mixtureSize : Int, muK : Array[RDD[Probability]], theta : Double) : (Array[Graph[SecondPhaseNode, Double]], Array[RDD[Probability]]) = 
	{
		val numberOfVar = train.count
		val numberOfObs = train.first._2.length
		val trainAndBelief = mixtureTree.map{tree => tree.vertices.map(v => (v._1.toDouble, v._2.belief)).join(train)}
		// Compute gamma_k(i)
		val muAndP = (trainAndBelief.zip(muK)).map{ case(frac, mu) => 
		{
			val param = frac.map(a => (1, a)).join(mu.map(m => (1, m)))
			// maybe should use a fold
			val observationProb = param.map{ case( genericKey, ((key, (prob, arr)), mu)) => (1, arr.map(v => Probability(mu.value * prob(v).value)))}
			observationProb.reduceByKey((probArr1, probArr2) =>  (probArr1.zip(probArr2)).map{ case (a,b) => Probability(a.value * b.value)})
		}}
		val sumGamma= muAndP.reduce((rdd1, rdd2) => (rdd1.join(rdd2)).map{ case(key, (probArr1, probArr2)) => (key, (probArr1.zip(probArr2)).map{case(a,b) => Probability(a.value + b.value)})})
		val gammaK = muAndP.map(t => (t.join(sumGamma)).map{ case (key,(muP, g)) => (muP.zip(g)).map{case(a,b) => Probability(a.value/b.value)}})
		val trainAndGammaK  = gammaK.zipWithIndex.map{ case(arr, i) => arr.map(v => (i.toDouble,v)).join(train).map{case(key, (sample, gK)) => (key, gK.zip(sample))}}

		val newMixtureTree = trainAndGammaK.map(t => {
		    val messageGraph = GHSWeightedGraph(t, sc)
			val GHSMwstGraph = GHSMwst(messageGraph)
		    val markovTree = markovTreeCreation(GHSMwstGraph)
			val markovTreeSetUp = learnWeightedParameters(markovTree, t)
			val evidence = EvidenceSet()	
			inference(markovTreeSetUp, evidence)	
		})
		// compute mu_k
		val newMuK = gammaK.map{ probArr => probArr.map{ prob => {
				val reduceProb = prob.reduce((a,b) => Probability(a.value + b.value))			
				Probability(reduceProb.value / numberOfObs)}}}

		val newTheta =  mixtureTree.map(tree => tree.vertices.map{case(key, node) => node.cpt(node.cpt.keys.min(Ordering.by[JointEvent, Double](e => e.variable)))}.
			reduce((a,b) => Probability(a.value + b.value))).reduce((a,b) => Probability(a.value + b.value)).value / (mixtureSize * numberOfVar) 
			
		if( theta - newTheta < 1)
			return (newMixtureTree, muK)
		else
			recEM(sc, train, newMixtureTree, mixtureSize, newMuK, newTheta)
	}
	
	def EM(sc: SparkContext, train : RDD[(Double, Array[Double])], fraction : Int) : (Array[Graph[SecondPhaseNode, Double]], Array[RDD[Probability]]) = 
	{
		//Initialization of T, parameters and mu
		val numberOfVar = train.count
		val mixtureTree = fractionalSet(train, fraction).map(fraction => {
		    val messageGraph = GHSGraph(fraction, sc)
			val GHSMwstGraph = GHSMwst(messageGraph)
		    val markovTree = markovTreeCreation(GHSMwstGraph)
			val markovTreeSetUp = learnParameters(markovTree, fraction)
			val evidence = EvidenceSet()	
			inference(markovTreeSetUp, evidence)	
		})
		val muK = mixtureTree.map(tree => tree.vertices.map(v => Probability(1D/5)))
		val theta =  mixtureTree.map(tree => tree.vertices.map{case(key, node) => node.cpt(node.cpt.keys.min(Ordering.by[JointEvent, Double](e => e.variable)))}.
			reduce((a,b) => Probability(a.value + b.value))).reduce((a,b) => Probability(a.value + b.value)).value / (fraction * numberOfVar)
		return recEM(sc, train, mixtureTree, fraction, muK, theta)
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
	
	def getInferedProbability(mixtureTree : Array[Graph[MarkovNode, Double]], evidence : EvidenceSet) : RDD[(VertexId, Map[Double, Probability])] =
	{
		val numberOfTree = mixtureTree.length
		val inferedMixture = mixtureTree.map(tree => inference(tree, evidence))
        val weightByTree = Array.fill(numberOfTree)(1D/numberOfTree)
        val beliefByTree = inferedMixture.map( tree => tree.vertices.map(e => (e._1, e._2.belief.map{ case (key, prob) => (key -> Probability(prob.value/numberOfTree))})))
        return beliefByTree.reduce((a,b) => a.join(b).map{ case(key, (map1, map2)) => (key, sumMapByKey(map1, map2))})
	}

}
