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
//~ import scalaz._ 

object Boruvka {
	
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
	
	def boruvkaDistAlgo[GraphType](graph : Graph[GraphType, Double]) : Set[Edge[Double]] = 
	{
		var setEdges = Set[Edge[Double]]()
		
		var setVertices = graph.vertices.map(x => Set(x._1))
		
		//~ var h = Heap[(VertexId, Node)]()
		
		while (setVertices.count.toInt > 1)
		{
			
			var test = setVertices.map
			{
				smallG =>
				graph.subgraph(e => smallG.contains(e.srcId) && !smallG.contains(e.dstId) ||
											smallG.contains(e.dstId) && !smallG.contains(e.srcId),
											(v,d) => true).edges.min()(Ordering.by(e => e.attr))
			}
			
			//~ test.foreach
			//~ {
				//~ edge =>
					//~ 
					//~ var src = edge.srcId
					//~ var dst = edge.dstId
					//~ 
					//~ var indexSrc = 0
					//~ var srcFound = false
					//~ var indexDst = 0
					//~ var dstFound = false
					//~ 
					//~ breakable
					//~ {
						//~ for(i <- 0 to setVertices.length - 1)
						//~ {
							//~ if (srcFound && dstFound)
								//~ break
							//~ else if (!srcFound && setVertices(i).contains(src)){
								//~ indexSrc = i
								//~ srcFound = true}
							//~ else if (!dstFound && setVertices(i).contains(dst)){
								//~ indexDst = i
								//~ dstFound = true}
						//~ }
					//~ }
//~ 
					//~ setVertices(indexSrc) = setVertices(indexSrc) ++ setVertices(indexDst)
					//~ setVertices = remove(setVertices(indexDst), setVertices)
//~ 
					//~ setEdges += edge	
			//~ }	
		}
		return setEdges
	}
}
