/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Boruvka's algorithm                                         //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import util.control.Breaks._
import org.apache.spark.graphx._
import graphicalLearning.DistributedGraph._
import org.apache.spark.rdd.RDD
//~ import scalaz._ 

object Boruvka {
	
	// Only RDD boruvka (should be optimized)
	def boruvkaRec(finalE : RDD[Edge[Double]], setVertices : RDD[(Long, Long)], setEdges : RDD[(Long, Edge[Double])], nodesLeft : Int) : RDD[Edge[Double]] = 
	{
		if (nodesLeft == 0) return finalE	
		else 
		{
			val minEdges = setVertices.join(setEdges).map{ case ( key1, (key2, edge)) => (key1, edge)}.reduceByKey((e1, e2) => if (e1.attr < e2.attr) e1 else e2)
			val newVertices = minEdges.map{ case (key, edge) => { val minId  = math.min(edge.srcId, edge.dstId)
				(minId,minId)}}
			val newEdges = minEdges.values.distinct
			val addedNodes = newEdges.count.toInt
			val lighterSetEdges = setEdges.subtract(minEdges)
			val newSetEdges = lighterSetEdges.join(minEdges).map{ case (key, (edge1, edge2)) => {
				val minId = math.min(edge2.srcId, edge2.dstId)
				 if(minId < key) (minId, edge1) else (key, edge1)}}

			boruvkaRec(finalE ++ newEdges, newVertices, newSetEdges, nodesLeft - addedNodes)
		}
	}
	
	def boruvkaDistAlgo[GraphType](graph : Graph[GraphType, Double]) : RDD[Edge[Double]] = 
	{
		val nbNodes = graph.vertices.count.toInt

		val setVertices = graph.vertices.map{ case (vid, _) => (vid, vid)}

		val finalE = graph.edges.filter(e => e != e)
		
		val setSrcEdges = graph.edges.map( e => (e.srcId, e))
		val setDstEdges = graph.edges.map( e => (e.dstId, e))
		val srcUdst = setSrcEdges union setDstEdges
		
		return boruvkaRec(finalE, setVertices, srcUdst, nbNodes - 1)
	}
	
	
	// borukva done mostly on local drive. RDD are used to find the minimum edge by using the subgraph method
	def remove(num: Set[Long], A: Array[Set[Long]]) = A diff Array(num)

	
	def boruvkaAlgo[GraphType](graph : Graph[GraphType, Double]) : Set[Edge[Double]] = 
	{
		var setEdges = Set[Edge[Double]]()
		
		var setVertices = graph.vertices.collect.map(x => Set(x._1))
		
		//~ var h = Heap[(VertexId, Node)]()
		
		while (setVertices.length > 1)
		{
			var edges = Set[Edge[Double]]()
			
			setVertices.foreach
			{
				smallG =>
				edges += graph.subgraph(e => smallG.contains(e.srcId) && !smallG.contains(e.dstId) ||
											smallG.contains(e.dstId) && !smallG.contains(e.srcId),
											(v,d) => true).edges.min()(Ordering.by(e => e.attr))
			}
			
			edges.foreach
			{
				edge =>
					
					var src = edge.srcId
					var dst = edge.dstId
					
					var indexSrc = 0
					var srcFound = false
					var indexDst = 0
					var dstFound = false
					
					breakable
					{
						for(i <- 0 to setVertices.length - 1)
						{
							if (srcFound && dstFound)
								break
							else if (!srcFound && setVertices(i).contains(src)){
								indexSrc = i
								srcFound = true}
							else if (!dstFound && setVertices(i).contains(dst)){
								indexDst = i
								dstFound = true}
						}
					}

					setVertices(indexSrc) = setVertices(indexSrc) ++ setVertices(indexDst)
					setVertices = remove(setVertices(indexDst), setVertices)

					setEdges += edge	
			}	
		}
		return setEdges
	}
}
