/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Creates a bayesian network                                  //
/////////////////////////////////////////////////////////////////   

package graphicalLearning

import graphicalLearning.Kruskal._

import Array._
import math.log
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.graphx._

object Bayesian_network
{
	
	// Could change those functions to run on rdd even more
	// See how to deal with the cast, create a class ? 
	
	//~ def entropy(l : org.apache.spark.mllib.linalg.Vector): Float = 
    //~ {
        //~ var length = l.size
        //~ var freq = l.toArray.groupBy(x=>x).mapValues(_.size.toFloat/length)
        //~ 
        //~ return freq.values.map{ x =>
            //~ -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    //~ }
   
	def entropy(l : Array[Double]): Float = 
    {
        var length = l.length
        var freq = l.groupBy(x=>x).mapValues(_.size.toFloat/length)
        
        return freq.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    }    
    
	def entropy(l : Array[Float]): Float = 
    {
        var length = l.length
        var freq = l.groupBy(x=>x).mapValues(_.size.toFloat/length)
        
        return freq.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    }
    //~ 
    //~ def entropy(l : List[Float]): Float = 
    //~ {
        //~ var length = l.length
        //~ var freq = l.groupBy(x=>x).mapValues(_.size.toFloat/length)
        //~ 
        //~ return freq.values.map{ x =>
            //~ -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    //~ }
    //~ 
    //~ def entropy(l : RDD[Float]): Float = 
    //~ {
        //~ var length = l.count
        //~ var freq = l.groupBy(x=>x).mapValues(_.size.toFloat/length)
        //~ 
        //~ return freq.values.map{ x =>
            //~ -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    //~ }
    //~ 
    //~ def entropy(l : RDD[(Float, Array[Float])]): Float = 
    //~ {
        //~ var length = l.take(1)(0)._2.length
        //~ var freq = l.take(1)(0)._2.groupBy(x=>x).mapValues(_.size.toFloat/length)
        //~ 
        //~ return freq.values.map{ x =>
            //~ -x * math.log(x) / math.log(2)}.reduce(_+_).toFloat
    //~ }
    
    def conditionalEntropy(variable : Array[Double], condition : Array[Double]): Float =
    {
		var length = variable.length
		var conjMap:Map[(Double,Double), Int] = Map()
		var pY = condition.groupBy(x=>x).mapValues(_.size.toFloat/length)
		
		for (i <- 0 to length - 1) 
		{
			if (conjMap.contains((variable(i), condition(i))))
				conjMap = conjMap.updated((variable(i), condition(i)), conjMap((variable(i), condition(i))) + 1)
			else
				conjMap += ((variable(i), condition(i)) -> 1)
		}
		var result = 0.toFloat
		conjMap.foreach
		{
			x =>
			if (x._2 == 0) result
			else
			{
				var normV = (x._2/length.toFloat)

				result = result - normV * (math.log((normV / pY(x._1._2)))/math.log(2)).toFloat
			}
		}	
		return result 
	}        
	 
    //~ def getCol(n: Int, a: Array[Array[Float]]) = a.map{_(n - 1)}
//~ 
    //~ def conditionalEntropy(variable : Array[Double], condition : Array[Double]): Float = {
        //~ var nbSamples = variable.length - 1
        //~ var freqMap = variable.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityVar = freqMap.size
        //~ var freqMap2 = condition.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityCond = freqMap2.size
        //~ var M = ofDim[Float](cardinalityVar, cardinalityCond)
//~ 
        //~ for (i <- 0 to nbSamples)
        //~ {
            //~ M(variable(i).toInt)(condition(i).toInt) = M(variable(i).toInt)(condition(i).toInt) + 1
        //~ }
        //~ 
        //~ M = M.map(_.map(_/nbSamples.toFloat))
        //~ 
        //~ var pY = new Array[Float](cardinalityCond)
        //~ 
        //~ for(i<-0 to cardinalityCond - 1) 
        //~ {
            //~ pY(i) = getCol(i+1, M).reduce(_+_)
        //~ }
        //~ 
        //~ return M.map{ l => 
            //~ var pYIndex = -1
            //~ l.map{x=>
                //~ pYIndex += 1
                //~ if (x == 0) x
                //~ else -x * math.log(x/pY(pYIndex))/math.log(2)}.reduce(_+_).toFloat
            //~ }.reduce(_+_).toFloat
    //~ }
//~ 
    //~ def conditionalEntropy(variable : List[Float], condition : List[Float]): Float = {
        //~ var nbSamples = variable.length - 1
        //~ var freqMap = variable.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityVar = freqMap.size
        //~ var freqMap2 = condition.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityCond = freqMap2.size
        //~ var M = ofDim[Float](cardinalityVar, cardinalityCond)
//~ 
        //~ for (i <- 0 to nbSamples)
        //~ {
            //~ M(variable(i).toInt)(condition(i).toInt) = M(variable(i).toInt)(condition(i).toInt) + 1
        //~ }
        //~ 
        //~ M = M.map(_.map(_/nbSamples.toFloat))
        //~ 
        //~ var pY = new Array[Float](cardinalityCond)
        //~ 
        //~ for(i<-0 to cardinalityCond - 1) 
        //~ {
            //~ pY(i) = getCol(i+1, M).reduce(_+_)
        //~ }
        //~ 
        //~ return M.map{ l => 
            //~ var pYIndex = -1
            //~ l.map{x=>
                //~ pYIndex += 1
                //~ if (x == 0) x
                //~ else -x * math.log(x/pY(pYIndex))/math.log(2)}.reduce(_+_).toFloat
            //~ }.reduce(_+_).toFloat
    //~ }
    
    def mutInfo(variable : RDD[LabeledPoint], condition : RDD[LabeledPoint]): Float = 
    {
        var result = entropy(variable.take(1)(0).features.toArray) - conditionalEntropy(variable.take(1)(0).features.toArray, condition.take(1)(0).features.toArray)
        return result
    } 
    
    def mutInfo(variable : Array[Double], condition : Array[Double]): Float = 
    {
        var result = entropy(variable) - conditionalEntropy(variable, condition)
        return result
    } 

    //~ def mutInfo(variable : List[Float], condition : List[Float]): Float = 
    //~ {
        //~ var result = entropy(variable) - conditionalEntropy(variable, condition)
        //~ return result
    //~ } 
//~ 
    //~ def mutInfo(variable :  RDD[(Float, Array[Float])], condition : RDD[(Float, Array[Float])]): Float = 
    //~ {
        //~ var result = entropy(variable.take(1)(0)._2) - conditionalEntropy(variable.take(1)(0)._2, condition.take(1)(0)._2)
        //~ return result
    //~ } 

    //~ def skelTree(samples : org.apache.spark.rdd.RDD[(Float, Array[Float])]) : Array[Array[Float]] = 
    //~ {
        //~ var nbNodes = samples.count.toInt
        //~ var M = ofDim[Float](nbNodes, nbNodes) 
        //~ 
        //~ for (i <- 0 to nbNodes-2)
        //~ {
            //~ for (j <- i+1 to nbNodes-1) 
            //~ {
                //~ //if (i == j) M(i)(j) = -10000
                //~ //else M(i)(j) = - mutInfo(samples(i), samples(j))
                //~ M(i)(j) = - mutInfo(samples.lookup(i)(0).toList, samples.lookup(j)(0).toList)
            //~ }
        //~ }
        //~ 
        //~ kruskalTree(M)
        //~ return M
        //~ 
    //~ }

    //~ def skelTree(samples : org.apache.spark.rdd.RDD[(Float, Array[Float])]) : Array[Array[Float]] = 
    //~ {
        //~ var nbNodes = samples.count.toInt
        //~ var M = ofDim[Float](nbNodes, nbNodes) 
        //~ 
        //~ for (i <- 0 to nbNodes-2)
        //~ {
            //~ for (j <- i+1 to nbNodes-1) 
            //~ {
                //~ //if (i == j) M(i)(j) = -10000
                //~ //else M(i)(j) = - mutInfo(samples(i), samples(j))
                //~ M(i)(j) = - mutInfo(samples.filter(a => a._1 == i.toFloat), samples.filter(a => a._1 == j.toFloat))
            //~ }
        //~ }
        //~ 
        //~ kruskalTree(M)
        //~ return M
        //~ 
    //~ }
    
    def skelTree(samples : RDD[LabeledPoint]) : Array[Array[Float]] = 
    {
        var nbNodes = samples.count.toInt
        var M = ofDim[Float](nbNodes, nbNodes) 
        
        for (i <- 0 to nbNodes-2)
        {
            for (j <- i+1 to nbNodes-1) 
            {
                //if (i == j) M(i)(j) = -10000
                //else M(i)(j) = - mutInfo(samples(i), samples(j))
                M(i)(j) = - mutInfo(samples.filter(a => a.label == i.toFloat), samples.filter(a => a.label == j.toFloat))
            }
        }
        
        //~ kruskalTree(M)
        return M
        
    }
}







