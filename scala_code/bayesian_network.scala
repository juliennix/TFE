/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Bayesian network                                            //
/////////////////////////////////////////////////////////////////   

package graphicalLearning

import Array._
import math.log

import graphicalLearning.Kruskal._

object Bayesian_network
{
    def entropy(l : List[Int]): Double = 
    {
        var length = l.length
        var freq = l.groupBy(x=>x).mapValues(_.size.toDouble/length)
        
        return freq.values.map{ x =>
            -x * log(x) / log(2)}.reduce(_+_).toDouble
    }
    
    def conditionalEntropy(variable : List[Int], condition : List[Int]): Double =
    {
		var length = variable.length
		var conjMap:Map[(Int,Int), Int] = Map()
		var pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/length)
		
		for (i <- 0 to length - 1) 
		{
			if (conjMap.contains((variable(i), condition(i))))
				conjMap = conjMap.updated((variable(i), condition(i)), conjMap((variable(i), condition(i))) + 1)
			else
				conjMap += ((variable(i), condition(i)) -> 1)
		}
		var result = 0.toDouble
		conjMap.foreach
		{
			x =>
			if (x._2 == 0) result
			else
			{
				var normV = (x._2.toDouble/length)

				result = result - normV * (log((normV / pY(x._1._2)))/log(2)).toDouble
			}
		}	
		return result 
	}
	
         
           
    //~ def getCol(n: Int, a: Array[Array[Float]]) = a.map{_(n - 1)}
//~ 
    //~ def conditionalEntropy(variable : List[Int], condition : List[Int]): Float =
    //~ {
        //~ var nbSamples = variable.length
        //~ var freqMap = variable.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityVar = freqMap.size
        //~ var freqMap2 = condition.groupBy(x=>x).mapValues(_.size.toFloat)
        //~ var cardinalityCond = freqMap2.size
        //~ var M = ofDim[Float](cardinalityVar, cardinalityCond)
//~ 
        //~ for (i <- 0 to nbSamples - 1)
        //~ {
            //~ M(variable(i))(condition(i)) = M(variable(i))(condition(i)) + 1
        //~ }
        //~ 
        //~ M = M.map(_.map(_/nbSamples.toFloat))
        //~ var pY = condition.groupBy(x=>x).mapValues(_.size.toFloat/nbSamples)
        //~ 
        //~ return M.map{ l => 
            //~ var pYIndex = -1
            //~ l.map{x=>
                //~ pYIndex += 1
                //~ if (x == 0) x
                //~ else - x * log(x/pY(pYIndex))/log(2)}.reduce(_+_)
            //~ }.reduce(_+_).toFloat
    //~ }

    def mutInfo(variable : List[Int], condition : List[Int]): Double = 
    {
        var result = entropy(variable) - conditionalEntropy(variable, condition)
        return result
    } 

    def skelTree(samples : List[List[Int]]) = 
    {
        var nbNodes = samples.length
        var M = ofDim[Double](nbNodes, nbNodes) 
        
        for (i <- 0 to nbNodes-2)
        {
            for (j <- i+1 to nbNodes-1) 
            {
                //if (i == j) M(i)(j) = -10000
                //else M(i)(j) = - mutInfo(samples(i), samples(j))
                M(i)(j) = - mutInfo(samples(i), samples(j))
            }
        }
        
        
        //~ for (i <- 0 to nbNodes-1)
        //~ {
            //~ for (j <- 0 to nbNodes-1) 
            //~ {
                //~ //if (i == j) M(i)(j) = -10000
                //~ //else M(i)(j) = - mutInfo(samples(i), samples(j))
                //~ M(i)(j) = - mutInfo(samples(i), samples(j))
            //~ }
        //~ }
        //~ 
        //~ var MTranspose = M.transpose
        //~ 
        //~ for (i <- 0 to nbNodes-1) 
        //~ {
            //~ for (j <- 0 to nbNodes-1) 
            //~ {
                //~ M(i)(j) = (M(i)(j) + MTranspose(i)(j))/2
            //~ }
        //~ }
 
        kruskalTree(M)
        
    }
}













