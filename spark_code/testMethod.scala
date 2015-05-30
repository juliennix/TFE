/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Test methods according to different scores                  //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexId
import graphicalLearning.MarkovTree._

import graphicalLearning.MixtureTree._
import graphicalLearning.MutualInfo._


object TestMethod extends Serializable
{	
	def KLDivergenceRDD(mixtureTree : Array[Graph[MarkovNode, Double]], test : RDD[(Double, Array[Double])], validation : RDD[(Double, Double)], numberOfSample: Int) : Double = 
	{
		val numberOfEvidence = test.count
		val numberOfTree = mixtureTree.length
		return mixtureTree.map(tree =>
		{
			val newVertices = tree.aggregateMessages[Double](
					  triplet => { 
						  triplet.sendToDst(triplet.srcId)
					  },
					  (a, b) => a
					)
			val probaRoot = tree.vertices.filter(e => e._2.level == 0D).map(t =>
				(1, t)).join(test.map(e => (1,e))).map{case (key, ((vid, markovNode), (obs, arr))) => 
					(obs, markovNode.cpt.getOrElse(JointEvent(arr(vid.toInt), arr(vid.toInt)), Probability( 1D/(numberOfSample+markovNode.cpt.keys.maxBy(jointEvent => 
						jointEvent.variable).variable))))}.cache
					
			val proba = newVertices.join(tree.vertices).map(t =>
				(1, t)).join(test.map(e => (1,e))).map{case (key, ((vid, (parentId, markovNode)), (obs, arr))) => 
					(obs, markovNode.cpt.getOrElse(JointEvent(arr(vid.toInt), arr(parentId.toInt)), Probability( 1D/(numberOfSample+markovNode.cpt.keys.maxBy(jointEvent => 
						jointEvent.variable).variable))))}.groupBy(x => x._1).map{case (key, probIter) => (key, probIter.reduce((a,b) => (key, Probability(a._2.value * b._2.value))))}.map{case(key, (key2, prob)) =>
							(key, prob)}.cache
			
			validation.join(probaRoot.join(proba).map{case(key, (probaR, probaN)) =>
				(key, probaR.value * probaN.value)}).map{case (key, (realProba, mixtureProba)) => 
				(math.log(realProba/mixtureProba))/math.log(2)}.reduce(_ + _)/numberOfEvidence
			
		}).reduce(_ + _)/numberOfTree
	}
	
	def getEvidenceFromTest(test : RDD[Array[Double]]): RDD[EvidenceSet] =
	{
		return test.map(arr => 
			EvidenceSet(arr.zipWithIndex.map{case(value, id) => (id.toLong, Evidence(value, Probability(1)))}.toMap))
	}
	
	def specialScore(inferedProb : RDD[(VertexId, Map[Double, Probability])], test : RDD[(Double, Array[Double])]) : Double = 
	{
		val probTest = test.map{ case (key, array) => (key.toLong, probability(array))}
		val entropy = inferedProb.map{ case(id, probMap) => (id, probMap.map{ case(k, prob) => (k -> Probability(prob.value * math.log(prob.value)/math.log(2)))})}.
			reduce{case ((key1, map1), (key2, map2)) => (key1, sumMapByKey(map1, map2))}._2.values.reduce((a,b) => Probability(a.value + b.value)).value
		val crossEntropy = inferedProb.join(probTest).map{ case(key, (map1, map2)) => (key, mulMapByKey(map1, map2.map{ case(k, prob) => (k -> Probability( -math.log(prob.value)/math.log(2)))}))}.
			reduce{case ((key1, map1), (key2, map2)) => (key1, sumMapByKey(map1, map2))}._2.values.reduce((a,b) => Probability(a.value + b.value)).value
		return - entropy + crossEntropy
	}		
}

