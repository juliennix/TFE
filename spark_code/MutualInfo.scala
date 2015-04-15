/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Creates a bayesian network                                  //
/////////////////////////////////////////////////////////////////   

package graphicalLearning

import Array.ofDim
import math.log
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object MutualInfo extends Serializable
{
    // Totally done through RDD using the cartesian product (may be heavy)
    def mutInfoRDD(samples : RDD[(Double, Array[Double])]) : RDD[((Double,Double), Double)] =
    {
		val precision = 8 // number of decimal digits
		val keyValue = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 < key2}
		return keyValue.map{ case ((key1, val1), (key2, val2)) => {
			((key1,key2) , truncateAt(entropy(val1) - conditionalEntropy(val1, val2), precision))
			}
		}
	}
	// Same but keeping the whole cartesian product (may be heavy)
    def fullMutInfoRDD(samples : RDD[(Double, Array[Double])]) : RDD[((Double,Double), Double)] =
    {
		val precision = 8 // number of decimal digits
		val keyValue = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 != key2}
		return keyValue.map{ case ((key1, val1), (key2, val2)) => {
			((key1,key2) , truncateAt( entropy(val1) - conditionalEntropy(val1, val2), precision))
			}
		}
	}
    // From labeledPoint, in order to retrieve the mutual information and as we cannot 
    // filter an RDD while accessing another, I had to retrieve the features (so the vector)
    // of the filtered RDD[LabeledPoint]
    def mutInfo(variable : RDD[LabeledPoint], condition : RDD[LabeledPoint]): Double = 
    {
		val precision = 8 // number of decimal digits
        val result = entropy(variable.first.features.toArray) - conditionalEntropy(variable.first.features.toArray, condition.first.features.toArray)
        return truncateAt(result, precision)
    } 

	// Wanted to compute in a more efficient way by passing the length and not computing multiple times and computing
	// the mutual information through RDD but as we cannot access at multiple part of one RDD, this is not used
    def mutInfo(length : Int, data : RDD[LabeledPoint], variableLabel : Long, conditionLabel : Long): Double = 
    {
		val precision = 8 // number of decimal digits
        val result = entropy(length, data, variableLabel) - conditionalEntropy(length, data, variableLabel, conditionLabel)
        return truncateAt(result, precision)
    } 
 
    // Used by the "after" graph in order to compute the mutual information knowing that each sample is stored
    // in the nodes of the graph in order to compute in parallele
    def mutInfo(variable : Array[Double], condition : Array[Double]): Double = 
    {
		val precision = 8 // number of decimal digits
        val result = entropy(variable) - conditionalEntropy(variable, condition)
        return truncateAt(result, precision)
    } 
     
	def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }     
    
    // I(X;Y|Z) = H(X|Z) - H(X|Y,Z)
    //          = H(X|Z) - H(X,Y,Z) + H(Y,Z)
    //~ def condMutInfoRDD(samples : RDD[(Double, Array[Double])]) : RDD[((Double,Double), Double)] =
    //~ {
		//~ val precision = 8 // number of decimal digits
		//~ val keyValue = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 < key2}
		//~ return keyValue.map{ case ((key1, val1), (key2, val2)) => {
			//~ ((key1,key2) , truncateAt(conditionalEntropy(val1, val2) - jointEntropy(val1, val2 + jointEntropy(val), precision))
			//~ }
		//~ }
	//~ }
    //~ 
    //~ def jointEntropy( variableArray : Array[Array[Double]])
    //~ {
		//~ 
	//~ }
    
	def entropy(l : Array[Double]): Double = 
    {
        val length = l.length
        val freq = l.groupBy(x=>x).mapValues(_.size.toDouble/length)
        
        return freq.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_)
    }           
	
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
    def conditionalEntropy(variable : Array[Double], condition : Array[Double]): Double =
    {
		val length = variable.length
		val pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/length)
		val conjMap = mapFunction(length, variable, condition)
		return conjMap.map
		{ 
			case (key, count) =>
			if (count == 0) 0D
			else
			{
				val norm = (count.toDouble / length)
				- norm * (math.log((norm / pY(key._2)))/math.log(2))
			}
		}.reduce(_ + _)
	}   
    
    def entropy(length : Int, data : RDD[LabeledPoint], variableLabel : Long): Double = 
    {
        val freq = data.filter(x => x.label == variableLabel).map(l => l.features.toArray.groupBy(x => x).mapValues(_.size.toDouble/length))
        return freq.map{ y => y.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_)}.first
    } 
    
    def conditionalEntropy(length : Int, data : RDD[LabeledPoint], variableLabel : Long, conditionLabel : Long): Double =
    {
		val variable = data.filter(x => x.label == variableLabel)
		val condition = data.filter(x => x.label == conditionLabel)
		
		// Maybe this could be on RDD even more but still 
		val conjMap = (variable.first.features.toArray zip condition.first.features.toArray).groupBy(x => x).mapValues(_.size.toDouble/length)
		val pY = condition.map(l => l.features.toArray.groupBy(x => x).mapValues(_.size.toDouble/length)).first

		return conjMap.map
		{
			x =>
			if (x._2 == 0) 0
			else
			{
				- x._2 * (math.log((x._2 / pY(x._1._2)))/math.log(2)).toDouble
			}
		}.reduce(_ + _)
	} 
    
    // Warning: only if your variables take consecutive values from 0 to n
    def getCol(n: Int, a: Array[Array[Double]]) = a.map{_(n - 1)}

    def conditionalEntropyArray(variable : Array[Double], condition : Array[Double]): Double = 
    {
        var nbSamples = variable.length
        var freqMap = variable.groupBy(x=>x).mapValues(_.size.toDouble)
        var cardinalityVar = freqMap.keys.max.toInt + 1
        var freqMap2 = condition.groupBy(x=>x).mapValues(_.size.toDouble)
        var cardinalityCond = freqMap2.keys.max.toInt + 1
        var M = ofDim[Double](cardinalityVar, cardinalityCond)

        for (i <- 0 to nbSamples - 1)
        {
            M(variable(i).toInt)(condition(i).toInt) = M(variable(i).toInt)(condition(i).toInt) + 1
        }
        
        M = M.map(_.map(_/nbSamples.toDouble))
        
        var pY = new Array[Double](cardinalityCond)
        
        for(i<-0 to cardinalityCond - 1) 
        {
            pY(i) = getCol(i+1, M).reduce(_+_)
        }
        // do the same
        //~ var pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/nbSamples)

        return M.map{ l => 
            var pYIndex = -1
            l.map{x=>
                pYIndex += 1
                if (x == 0) x
                else -x * math.log(x/pY(pYIndex))/math.log(2)}.reduce(_+_)
            }.reduce(_+_)
    }
    

    
}







