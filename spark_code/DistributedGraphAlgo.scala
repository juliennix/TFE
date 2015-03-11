/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Manages the creation and techniques on distr. graph         //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import graphicalLearning.MutualInfo._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

object DistributedGraph
{
	
	def createGraph(weightMatrix : Array[Array[Float]], sc : SparkContext) :  Graph[Long, Double] = 
	{
        var nbNodes = weightMatrix.size
        
        var verticesArray = new Array[(VertexId, Long)](nbNodes)
        var edgesArray = new Array[Edge[Double]](nbNodes*nbNodes)
			for (i <- 0 to nbNodes-2)
			{
				verticesArray(i) = (i.toLong, i.toLong)
				for (j <- i+1 to nbNodes-1) 
				{
					edgesArray((i * nbNodes) - ((i+1)*(i+1) - (i+1))/2 + j - (i + 1)) = Edge(i.toLong, j.toLong, weightMatrix(i)(j))
				}
			}
        
        val vertices: RDD[(VertexId,Long)] = sc.parallelize(verticesArray)
        // Filter the edges with mutingo == 0
        val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(e => e.attr == 0))
		val graph = Graph(vertices, edges)
		return graph
	}
	
	def directSlowInfoGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Long, Double] = 
	{
        var nbNodes = samples.count.toInt
        
        // Warning, if the vertexId are the label, be sure to have label in a consecutive order from 0 to n
        val vertices: RDD[(VertexId, Long)] = samples.map {x =>
			(x.label.toLong, x.label.toLong)}
			
        var edgesArray = new Array[Edge[Double]]((nbNodes*(nbNodes - 1 ))/2)
		for (i <- 0 to nbNodes-2)
		{
			for (j <- i+1 to nbNodes-1) 
			{
				var variable = samples.filter(a => a.label == i.toFloat)
				var condition = samples.filter(a => a.label == j.toFloat)
				var mutualInfo = - mutInfo(variable, condition)
				if (mutualInfo != 0)
					edgesArray((i * nbNodes) - ((i+1)*(i+1) - (i+1))/2 + j - (i + 1)) = Edge(i.toLong, j.toLong, mutualInfo)
			}
		}
       // Filter the edges with mutingo == 0
        val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(e => e.attr == 0))
		val graph = Graph(vertices, edges)
		return graph
	}
	
	def afterInfoGraph(samples : RDD[LabeledPoint], sc : SparkContext) : Graph[Array[Double], Double] = 
	{
        var nbNodes = samples.count.toInt
        var weight = 0
        
        // Warning, if the vertexId are the label, be sure to have label in a consecutive order from 0 to n
        val vertices: RDD[(VertexId,Array[Double])] = samples.map {x =>
			(x.label.toLong, x.features.toArray)}
			
        var edgesArray = new Array[Edge[Double]]((nbNodes*(nbNodes - 1 ))/2)
		for (i <- 0 to nbNodes-2)
		{
			for (j <- i+1 to nbNodes-1) 
			{
				edgesArray((i * nbNodes) - ((i+1)*(i+1) - (i+1))/2 + j - (i + 1)) = Edge(i.toLong, j.toLong, (weight))
			}
		}
		// Filter the edges with mutingo == 0
        val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(e => e.attr == 0))
		var graph = Graph(vertices,edges)
		val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet)) 
		return weigthGraph
	}
		
	val computeMutInfo = (triplet : EdgeTriplet[Array[Double], Double]) =>
			- mutInfo(triplet.srcAttr, triplet.dstAttr)
			
			
	def afterFullGraph(samples : RDD[LabeledPoint], sc : SparkContext) : Graph[Array[Double], Double] = 
	{
        var nbNodes = samples.count.toInt
        var weight = 0
        // Warning, if the vertexId are the label, be sure to have label in a consecutive order from 0 to n
        val vertices: RDD[(VertexId,Array[Double])] = samples.map {x =>
			(x.label.toLong, x.features.toArray)}
        var edgesArray = new Array[Edge[Double]](nbNodes * nbNodes)
		for (i <- 0 to nbNodes-1)
		{
			for (j <- 0 to nbNodes-1) 
			{
				if ( i != j)
					edgesArray(i * nbNodes + j) = Edge(i.toLong, j.toLong, (weight))
			}
		}
        // Filter the edges with mutingo == 0
        val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(e => e.attr == 0))
		val graph = Graph(vertices,edges)
		val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		return weigthGraph
	}
		

	def fastGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Array[Double], Double] = 
	{
        val nbNodes = samples.count.toInt
			
        val vertices: RDD[(VertexId, Array[Double])] = samples.map {x =>
			(x.label.toLong, x.features.toArray)}
			
		val label = samples.map(x =>
			x.label)
			
		val cartesianLabel = label.cartesian(label).filter{case (label1, label2) => label1 < label2}

		val edges = cartesianLabel.map{ x =>
			Edge(x._1.toLong, x._2.toLong, 0.toDouble)
		}
		
		val graph = Graph(vertices, edges)
		
		val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		return weigthGraph
	}
	
	def fastGraphSecond(samples :  RDD[(Double, Array[Double])], sc : SparkContext) :  Graph[Double, Double] = 
	{			
		val mutualInfo = mutInfoRDD(samples)
        val vertices: RDD[(VertexId, Double)] = samples.map { case(k,v) =>
			(k.toLong, k)}
		val edges = mutualInfo.map{ case ((key1, key2), weight) => Edge(key1.toLong, key2.toLong, - weight)}
		val graph = Graph(vertices, edges)
		return graph
	}
	
	def fastFullGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Array[Double], Double] = 
	{
        val nbNodes = samples.count.toInt
			
        val vertices: RDD[(VertexId, Array[Double])] = samples.map {x =>
			(x.label.toLong, x.features.toArray)}
			
		val label = samples.map(x =>
			x.label)
			
		val cartesianLabel = label.cartesian(label).filter{case (label1, label2) => label1 != label2}

		val edges = cartesianLabel.map{ x =>
			Edge(x._1.toLong, x._2.toLong, 0.toDouble)
		}
		
		val graph = Graph(vertices, edges)
		
		val weigthGraph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		return weigthGraph
	}
}
