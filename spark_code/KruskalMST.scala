/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perfom kruskal algorithm                                    //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.graphx._
import collection.mutable.Set


import graphicalLearning.Bayesian_network._
import graphicalLearning.DistributedGraph._

object Kruskal {
 	
	def remove(num: Set[Long], A: Array[Set[Long]]) = A diff Array(num)

	def kruskal(graph : Graph[Node, Float]) : Set[Edge[Float]] =
	{
		var setEdges = Set[Edge[Float]]()
	
		var setVertices = graph.vertices.collect.map(x => Set(x._1))
		
		graph.edges.sortBy(_.attr).collect.foreach
		{
			edge =>	
			//~ // not allowed return in spark 
			//~ if (setVertices.length > 1)
				//~ return setEdges
				
			if (setVertices.length > 1)
			{
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
				if (indexSrc != indexDst)
				{
					setVertices(indexSrc) = setVertices(indexSrc) ++ setVertices(indexDst)
					setVertices = remove(setVertices(indexDst), setVertices)
					setEdges += edge
				}
			}
		}
		return setEdges	 
	}
	
	 
	
	
	 
	def mst(edge : Edge[Float]) = 
	{
		//~ (setVertices.contains(edge.srcId), setVertices.contains(edge.dstId))
		//~ match 
		//~ {
			//~ case (true, true)   => None // Cycle
			//~ case (false, true)  => { 	setVertices += Set(edge.srcId, edge.dst.Id)
								//~ setEdges += edge }
			//~ case (true, false)  =>
			//~ case (false, false) =>					
		//~ }
	}


	//~ case class Edge[A](v1:A, v2:A, weight:Double)
 //~ 
	//~ type LE[A] = List[Edge[A]]
	//~ type OS[A] = Option[Set[A]]
 //~ 
	//~ class SetMap[A](data:Map[A,collection.mutable.Set[A]] = Map[A,collection.mutable.Set[A]]()) 
	//~ {
		//~ import collection.mutable.Set
 //~ 
		//~ //checks if a value exists
		//~ def contains(a:A) = data.contains(a);
	 //~ 
		//~ //checks if two values are in the same Set
		//~ def sameSet(repr1:A, repr2:A) = data.get(repr1) == data.get(repr2)
	 //~ 
		//~ //adds a new Set with one initial value
		//~ def add(a:A) = if (contains(a)) this else new SetMap(data + (a -> Set(a)))
	 //~ 
		//~ //adds a new value to an existing Set (specified by repr)
		//~ def insert(a:A, repr:A) = if (contains(a) || ! contains(repr)) this else 
		//~ {
		  //~ val set = data(repr)
		  //~ set += a
		  //~ new SetMap(data + (a -> set))
		//~ }
	 //~ 
		//~ //merges two sets (specified by two representants)
		//~ def merge(repr1:A, repr2:A) = if(! contains(repr1) || ! contains(repr2)) this else
		//~ {
			//~ val set1 = data(repr1)
			//~ val set2 = data(repr2)
			//~ if(set1 eq set2) this else 
			//~ {
				//~ set1 ++= set2
				//~ new SetMap(data ++ set2.map(a => a -> set1))
			//~ }
		//~ }
	//~ }
 //~ 
	//~ def kruskal[A](list : LE[A]) = list.sortBy(_.weight).foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
	 //~ 
	//~ def kruskal[A](list : RDD[Edge[A]]) = list.top(list.count.toInt)(Ordering.by(e => e.weight)).foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
	//~ 
	//~ def kruskal[A](list : RDD[Edge[A]]) = list.sortBy(_.weight).collect.foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
	 //~ 
	//~ def mst[A](t:(LE[A], SetMap[A]), e:Edge[A]) = 
	//~ {
		//~ val ((es, sets), Edge(p,q,_)) = (t,e)
		//~ (sets.contains(p), sets.contains(q)) match 
		//~ {
			//~ case (false, false) => (e :: es, sets.add(p).insert(q,p))
			//~ case (true, false) => (e :: es, sets.insert(q,p))
			//~ case (false, true) => (e :: es, sets.insert(p,q))
			//~ case (true,true) => if (sets.sameSet(p,q)) (es, sets) //Cycle
								//~ else (e :: es, sets.merge(p,q))
		//~ }
	//~ }


	//~ def kruskalTree(weightMatrix : Array[Array[Float]]) = 
	//~ {
		//~ var nbNodes = weightMatrix.size
		//~ var listEdge = List[Kruskal.Edge[Symbol]]()
		//~ for (i <- 0 to nbNodes-2)
		//~ {
			//~ for (j <- i+1 to nbNodes-1) 
			//~ {
				//~ listEdge = listEdge ::: List(Edge(Symbol(i.toString),Symbol(j.toString),weightMatrix(i)(j)))
			//~ }
		//~ }
		//~ println(kruskal(listEdge))
	//~ }
	
	//~ def kruskalTree(samples : RDD[LabeledPoint], sc : SparkContext) = 
	//~ {
		//~ var nbNodes = samples.count.toInt
		//~ var listEdge = List[Kruskal.Edge[Symbol]]()
		//~ for (i <- 0 to nbNodes-2)
		//~ {
			//~ for (j <- i+1 to nbNodes-1) 
			//~ {
				//~ listEdge = listEdge ::: List(Edge(Symbol(i.toString),Symbol(j.toString),
				//~ - mutInfo(samples.filter(a => a.label == i.toFloat), samples.filter(a => a.label == j.toFloat))))
			//~ }
		//~ }
		//~ var LEP = sc.parallelize(listEdge)
		//~ println(kruskal(LEP))
	//~ }
 
}
