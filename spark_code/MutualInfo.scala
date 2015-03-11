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

object MutualInfo
{

	def entropy(length : Int, data : RDD[LabeledPoint], variableLabel : Long): Double = 
    {
        val freq = data.filter(x => x.label == variableLabel).map(l => l.features.toArray.groupBy(x => x).mapValues(_.size.toDouble/length))
        return freq.map{ y => y.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_)}.first
    }  
      
	def entropy(l : Array[Double]): Double = 
    {
        val length = l.length
        val freq = l.groupBy(x=>x).mapValues(_.size.toDouble/length)
        
        return freq.values.map{ x =>
            -x * math.log(x) / math.log(2)}.reduce(_+_)
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
			x =>
			if (x._2 == 0) 0
			else
			{
				val norm = (x._2.toDouble / length)
				- norm * (math.log((norm / pY(x._1._2)))/math.log(2))
			}
		}.reduce(_ + _)
	}   
    
    def mutInfoRDD(samples : RDD[(Double, Array[Double])]) : RDD[((Double,Double), Double)] =
    {
		val keyValue = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 < key2}
		return keyValue.map{ case ((key1, val1), (key2, val2)) => {
			((key1,key2) , entropy(val1) - conditionalEntropy(val1, val2))
			}
		}
	}
    
    def mutInfo(variable : RDD[LabeledPoint], condition : RDD[LabeledPoint]): Double = 
    {
        val result = entropy(variable.first.features.toArray) - conditionalEntropy(variable.first.features.toArray, condition.first.features.toArray)
        return result
    } 
    
    def mutInfo(length : Int, data : RDD[LabeledPoint], variableLabel : Long, conditionLabel : Long): Double = 
    {
        val result = entropy(length, data, variableLabel) - conditionalEntropy(length, data, variableLabel, conditionLabel)
        return result
    } 
    
    def mutInfo(variable : Array[Double], condition : Array[Double]): Double = 
    {
        val result = entropy(variable) - conditionalEntropy(variable, condition)
        return result
    } 

    //~ def skelTree(samples : org.apache.spark.rdd.RDD[(Float, Array[Float])]) : Array[Array[Float]] = 
    //~ {
        //~ var nbNodes = samples.count.toInt
        //~ var M = ofDim[Double](nbNodes, nbNodes) 
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

    //~ def skelTree(samples : org.apache.spark.rdd.RDD[(Float, Array[Float])]) : Array[Array[Double]] = 
    //~ {
        //~ var nbNodes = samples.count.toInt
        //~ var M = ofDim[Double](nbNodes, nbNodes) 
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
    
    def skelTree(samples : RDD[LabeledPoint]) : Array[Array[Double]] = 
    {
        val nbNodes = samples.count.toInt
        val M = ofDim[Double](nbNodes, nbNodes) 
        
        for (i <- 0 to nbNodes-2)
        {
            for (j <- i+1 to nbNodes-1) 
            {

                M(i)(j) = - mutInfo(samples.filter(a => a.label == i.toFloat), samples.filter(a => a.label == j.toFloat))
            }
        }
        //~ kruskalTree(M)
        return M
        
    }
    
    //~ def skelTree(samples : RDD[LabeledPoint]) : Array[Array[Double]] = 
    //~ {
        //~ var nbNodes = samples.count.toInt
        //~ var M = ofDim[Double](nbNodes, nbNodes) 
        //~ 
        //~ for (i <- 0 to nbNodes-2)
        //~ {
            //~ for (j <- i+1 to nbNodes-1) 
            //~ {
                //~ //if (i == j) M(i)(j) = -10000
                //~ //else M(i)(j) = - mutInfo(samples(i), samples(j))
                //~ M(i)(j) = - mutInfo(nbNodes, samples, i.toFloat, j.toFloat)
            //~ }
        //~ }
        //~ 
        //~ kruskalTree(M)
        //~ return M
        //~ 
    //~ }
    
    	// Warning: only if your variables take consecutive values from 0 to n
    def getCol(n: Int, a: Array[Array[Float]]) = a.map{_(n - 1)}

    def conditionalEntropyArray(variable : Array[Double], condition : Array[Double]): Float = {
        var nbSamples = variable.length - 1
        var freqMap = variable.groupBy(x=>x).mapValues(_.size.toFloat)
        var cardinalityVar = freqMap.size
        var freqMap2 = condition.groupBy(x=>x).mapValues(_.size.toFloat)
        var cardinalityCond = freqMap2.size
        var M = ofDim[Float](cardinalityVar, cardinalityCond)

        for (i <- 0 to nbSamples)
        {
            M(variable(i).toInt)(condition(i).toInt) = M(variable(i).toInt)(condition(i).toInt) + 1
        }
        
        M = M.map(_.map(_/nbSamples.toFloat))
        
        var pY = new Array[Float](cardinalityCond)
        
        for(i<-0 to cardinalityCond - 1) 
        {
            pY(i) = getCol(i+1, M).reduce(_+_)
        }
        
        return M.map{ l => 
            var pYIndex = -1
            l.map{x=>
                pYIndex += 1
                if (x == 0) x
                else -x * math.log(x/pY(pYIndex))/math.log(2)}.reduce(_+_).toFloat
            }.reduce(_+_).toFloat
    }
    
}







