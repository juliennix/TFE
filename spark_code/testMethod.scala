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
	def KLDivergence(mixtureTree : Array[Graph[MarkovNode, Double]], test : RDD[Array[Double]], validation : RDD[(Double, Double)], numberOfSample: Int, sc : SparkContext) : Double = 
	{
		val proba = new Array[(Double, Double)](test.count.toInt)
		val numberOfTree = mixtureTree.length
		val evidenceSetRDD = getEvidenceFromTest(test)
		val numberOfEvidence = evidenceSetRDD.count
		val evidencePerObs = evidenceSetRDD.zipWithIndex
        for (i <- 0 to (numberOfEvidence.toInt-1))
        {
			val evidenceSet = evidencePerObs.filter(e => e._2 == i.toLong).flatMap(e => e._1.evidences.map{case(key, evidence) => (key, evidence)})
			proba(i) = (i.toDouble, 
			
			mixtureTree.map(tree => 
			{
				val verticesObs = tree.vertices.join(evidenceSet)
				val newTree = Graph(verticesObs, tree.edges)
				val evidenceVertices = newTree.aggregateMessages[JointEvent](
				  triplet => { 
				      triplet.sendToDst(JointEvent(triplet.dstAttr._2.event, triplet.srcAttr._2.event))
				  },
				  (a, b) => a
				)
				val paramAndEvidence = evidenceVertices.join(tree.vertices)
				val probaRoot = newTree.vertices.filter(e => e._2._1.level == 0D).map{case(vid, (markovNode, evidence)) => markovNode.cpt.getOrElse(JointEvent(evidence.event, evidence.event), Probability(1D/(numberOfSample+markovNode.cpt.keys.maxBy(jointEvent => jointEvent.variable).variable)))}
				paramAndEvidence.map{case (vid, (jointEvent, markovNode)) => markovNode.cpt.getOrElse(jointEvent, Probability( 1D/(numberOfSample+markovNode.cpt.keys.maxBy(jointEvent => jointEvent.variable).variable)))}.map(p => p.value).reduce(_ * _) * probaRoot.first.value}).reduce(_ + _)/numberOfTree
			)
		} 
		val probaRDD = sc.parallelize(proba)
		val score = validation.join(probaRDD).map{case (key, (realProba, mixtureProba)) => (math.log(realProba/mixtureProba))/math.log(2)}.reduce(_ + _)/numberOfEvidence
		return score
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

