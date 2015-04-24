/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Test methods according to different scores                  //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.VertexId
import graphicalLearning.MarkovTree._

import graphicalLearning.MixtureTree._
import graphicalLearning.MutualInfo._


object TestMethod extends Serializable
{
	
        def KLDivergence(inferedProb : RDD[(VertexId, Map[Double, Probability])], test : RDD[(Double, Array[Double])]) : Double = 
        {
			val probTest = test.map{ case (key, array) => (key.toLong, probability(array))}
			val entropy = inferedProb.map{ case(id, probMap) => (id, probMap.map{ case(k, prob) => (k -> Probability(prob.value * math.log(prob.value)/math.log(2)))})}.
				reduce{case ((key1, map1), (key2, map2)) => (key1, sumMapByKey(map1, map2))}._2.values.reduce((a,b) => Probability(a.value + b.value)).value
			val crossEntropy = inferedProb.join(probTest).map{ case(key, (map1, map2)) => (key, mulMapByKey(map1, map2.map{ case(k, prob) => (k -> Probability( -math.log(prob.value)/math.log(2)))}))}.
				reduce{case ((key1, map1), (key2, map2)) => (key1, sumMapByKey(map1, map2))}._2.values.reduce((a,b) => Probability(a.value + b.value)).value
			return - entropy + crossEntropy
		}
}
