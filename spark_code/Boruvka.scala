/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Boruvka's algorithm                                         //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext._
import scala.util.control.Breaks._
import org.apache.spark.graphx._
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
//~ import scalaz._ 

case class NewIdVertex(id : VertexId, hasChanged : Boolean)

object Boruvka extends Serializable
{
	// Only RDD boruvka (could be optimized)
	def isEmpty[T](rdd : RDD[T]) = 
	{
		rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_) 
	}
	
	def minEdge(edge1 : Edge[Double], edge2 : Edge[Double]) : Edge[Double] =
	{
		if(edge1.attr < edge2.attr) edge1
		else if (edge1.attr == edge2.attr)
		{
			if(edge1.srcId < edge2.srcId) edge1
			else if(edge1.srcId == edge2.srcId)
			{
				if(edge1.dstId < edge2.dstId) edge1
				else edge2
				
			}
			else edge2
		}
		else
			edge2
	}
	
	def updateId(idVertex : RDD[(VertexId, (VertexId, VertexId))]) : RDD[(VertexId, VertexId)] =
	{
		val destVertex = idVertex.map{ case (s, (d, w)) => (d, (s, w))}.cache()
		val update = destVertex.reduceByKey((a,b) => if(a._2 < b._2) a else b).cache()
		val updateS = idVertex.join(update).map{ case (src, ((dst, w1), (newD, minW))) => (src, (dst, math.min( w1, minW)))}.cache()
		val newS = (idVertex.subtractByKey(updateS) union updateS).cache
		val nn = destVertex.join(update).join(updateS).map{ case (dst, (((src,w),(dst2, w2)), (dst3, w3))) => (src, (dst, math.min(w2,w3)))}
		val updatedAll = newS.join(nn).map{ case (src, ((dst1, w1), (dst2, w2))) =>  
				if(w1 < w2) (src, (dst1,  NewIdVertex(w1, true)))
				else if(w1 > w2) (src, (dst1,  NewIdVertex(w2, true)))
				else (src, (dst1, NewIdVertex(w1, false)))}.cache
		if(isEmpty(updatedAll.filter{ case (src, (dst, newIdVertex)) => newIdVertex.hasChanged}))
			return updatedAll.map{ case (src, (dst, newIdVertex)) => (src, newIdVertex.id)}
		else
		{
			updateId(updatedAll.map{ case (src, (dst, newIdVertex)) => (src, (dst, newIdVertex.id))})
		}
	}

	def boruvkaRec(finalE : RDD[Edge[Double]], setVertices : RDD[(Long, Long)], setEdges : RDD[(Long, Edge[Double])], nodesLeft : Int) : RDD[Edge[Double]] = 
	{
		if (nodesLeft == 0) return finalE	
		else 
		{
			val minEdges = setVertices.join(setEdges).map{ case ( key1, (key2, edge)) => (key1, edge)}.reduceByKey((e1, e2) => minEdge(e1, e2))
			val idVertex = minEdges.map{ case (key, edge) => if (edge.srcId == key) (key, (edge.dstId, math.min(key, edge.dstId))) else (key, (edge.srcId, math.min(key, edge.srcId)))}
			val newId = updateId(idVertex)
			val newVertices = updateId(idVertex).map{ case (src, id) => (id, id)}
			val newEdges = minEdges.values.distinct
			val addedEdges = newEdges.count.toInt
			val lighterSetEdges = setEdges.subtract(minEdges)
			val joinedSrc = lighterSetEdges.join(newId).map{ case (oldId, (edge, srcId)) => if (edge.srcId == oldId) (edge.dstId, (edge, srcId)) else (edge.srcId, (edge, srcId))}
			val newSetEdges = joinedSrc.join(newId).filter{ case (oldId, ((edge, srcId), dstId)) => srcId != dstId}.map{ case (oldId, ((edge, srcId), dstId)) =>
				 if(oldId == edge.srcId) (dstId, Edge(dstId, srcId, edge.attr)) else (dstId, Edge(srcId, dstId, edge.attr))}

			boruvkaRec(finalE ++ newEdges, newVertices, newSetEdges, nodesLeft - addedEdges)
		}
	}
	
	def boruvkaDistAlgo[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double] = 
	{
		val nbNodes = graph.vertices.count.toInt

		val setVertices = graph.vertices.map{ case (vid, _) => (vid, vid)}

		val finalE = graph.edges.filter(e => e != e)
		
		val setSrcEdges = graph.edges.map( e => (e.srcId, e))
		val setDstEdges = graph.edges.map( e => (e.dstId, e))
		val setEdges = setSrcEdges union setDstEdges
		
		return Graph(graph.vertices, boruvkaRec(finalE, setVertices, setEdges, nbNodes - 1))
	}

	//~ def boruvkaDistAlgo[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double] = 
	//~ {
		//~ val nbNodes = graph.vertices.count.toInt
//~ 
		//~ val setVertices = graph.vertices.map{ case (vid, _) => (vid, vid)}
//~ 
		//~ val finalE = graph.edges.filter(e => e != e)
		//~ 
		//~ val setEdges = graph.edges.map( e => (e.srcId, e))
		//~ 
		//~ return Graph(graph.vertices, boruvkaRec(finalE, setVertices, setEdges, nbNodes - 1))
	//~ }
	
	
	// borukva done mostly on local drive. RDD are used to find the minimum edge by using the subgraph method
	def remove(num: Set[Long], A: Array[Set[Long]]) = A diff Array(num)

	def boruvkaAlgo[VD: ClassTag](graph : Graph[Double, Double]) : Graph[Double, Double] = 
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
											(v,d) => true).edges.min()(Ordering.by(e => (e.attr, e.srcId, e.dstId)))
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
							if (!srcFound && setVertices(i).contains(src)){
								indexSrc = i
								srcFound = true}
							if (!dstFound && setVertices(i).contains(dst)){
								indexDst = i
								dstFound = true}
						}
					}
					
					if(indexSrc != indexDst) 
					{
						setVertices(indexSrc) = setVertices(indexSrc) ++ setVertices(indexDst)
						setVertices = remove(setVertices(indexDst), setVertices)
	
						setEdges += edge
					}
			}	
		}
		return graph.subgraph(e => setEdges.contains(e), (v,d) => true)
	}
}
