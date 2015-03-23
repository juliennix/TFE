/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perfom kruskal algorithm                                    //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.graphx._
import collection.mutable.Set


import graphicalLearning.MutualInfo._
import graphicalLearning.DistributedGraph._

object Kruskal {
 	// Kruskal algortihm in a functional fashion with only the first minimum weight edge taken on the local drive.
	def recKruskal[GraphType](edges : RDD[Edge[Double]], finalE : RDD[Edge[Double]], vertices : RDD[(Long,Long)], length : Int, i : Int = 0) : RDD[Edge[Double]] =
	{
		if (i == length) return finalE
		else{
			val edge = edges.first
			val edgeRDD = edges.filter(e => e == edge)
			val keySrc = vertices.lookup(edge.srcId).head
			val keyDst = vertices.lookup(edge.dstId).head
			val newKey = math.min(keySrc, keyDst).toLong
			val old = math.max(keySrc,keyDst)
			if(keySrc == keyDst) recKruskal(edges.filter(e => e != edge), finalE, vertices, length, i)
			else
			{
				val newSetV : RDD[(Long,Long)] = vertices.map{ case (key, subkey) => if (subkey == old) (key,newKey) else (key, subkey)}
				recKruskal(edges.filter(e => e != edge), finalE ++ edgeRDD, newSetV, length, i + 1)
			}
		}
	} 
	
	def kruskalDist[GraphType](graph : Graph[GraphType, Double]) : RDD[Edge[Double]] =
	{
		val nbNodes = graph.vertices.count.toInt
		val setVertices = graph.vertices.map{ case ( id,_)=> (id,id)}
		val setEdges = graph.edges.filter( x => x != x)
		
		return recKruskal(graph.edges.sortBy(_.attr), setEdges, setVertices, nbNodes - 1) 
	}
 	
 	// Here, we collect the sorted edges in order to loop on those and perform transformation on RDD (as we cannot loop on a RDD and access others)
 	def containV(edges : Array[Edge[Double]], setEdges : Set[Edge[Double]], setVertices : RDD[Set[Long]]) : Set[Edge[Double]] =
	{
		if (setVertices.count == 1)	return setEdges
		
		val edge = edges.head
		val src = edge.srcId
		val dst = edge.dstId 
		val setDst = setVertices.filter(set => set.contains(dst)).first
		val newVertices = setVertices.map(set =>	
			if (set.contains(src) && !set.contains(dst)) set ++ setDst
			else set).filter(set => (set.contains(src) && set.contains(dst)) || (!set.contains(src) && !set.contains(dst)))  
		val newEdges = setEdges ++ Set(edge)
		containV(edges.drop(1), newEdges, newVertices)		
	}
	def kruskalSecond[GraphType](graph : Graph[GraphType, Double]) : Set[Edge[Double]] =
	{
		val setEdges = Set[Edge[Double]]()
	
		val setVertices = graph.vertices.map{ case (vid, _) => Set(vid)}
		
		val edges = graph.edges.sortBy(_.attr).collect

		return containV(edges, setEdges, setVertices)	 
	} 	
 	
 	
 	// Here, sorting the edge is done on RDD but the result is collected, as the vertices thus, the computation is done mostly on the local drive
	def remove(num: Set[Long], A: Array[Set[Long]]) = A diff Array(num)

	def kruskal[GraphType](graph : Graph[GraphType, Double]) : Set[Edge[Double]] =
	{
		var setEdges = Set[Edge[Double]]()
	
		var setVertices = graph.vertices.collect.map(x => Set(x._1))
		
		graph.edges.sortBy(_.attr).collect.foreach
		{
			edge =>	
			{
				if (setVertices.length == 1)
					return setEdges
					
				val src = edge.srcId
				val dst = edge.dstId
				
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
	
	//~ def recKruskal[GraphType](edges : RDD[Edge[Double]], finalE : RDD[Edge[Double]], vertices : RDD[(Long,Long)], length : Int, i : Int = 0) : RDD[Edge[Double]] =
	//~ {
		//~ 
		//~ 
		//~ if (i == length) return false
		//~ 
		//~ }
	//~ } 
	//~ 
	//~ 
	//~ 
	//~ def kruskalDist[GraphType](graph : Graph[GraphType, Double]) : RDD[Edge[Double]] =
	//~ {
		//~ val nbNodes = graph.vertices.count.toInt
		//~ val setVertices = graph.vertices.map{ case ( id,_)=> (id,id)}
		//~ 
		//~ val sortedEdges = graph.edges.sortBy(_.attr).cache
		//~ 
		//~ val setSrcEdges = graph.sortedEdges.map( e => (e.srcId, e))
		//~ val setDstEdges = graph.sortedEdges.map( e => (e.dstId, e))
		//~ val srcUdst = setSrcEdges union setDstEdges
		//~ 
//~ 
		//~ return recKruskal(srcUdst, setVertices, nbNodes)
	//~ }

	//code of Daniel Gonau//
	// https://dgronau.wordpress.com/2010/11/28/nochmal-kruskal
	case class EdgeKruskal[A](v1:A, v2:A, weight:Double)
 
	type LE[A] = List[EdgeKruskal[A]]
	type OS[A] = Option[Set[A]]
 
	class SetMap[A](data:Map[A,collection.mutable.Set[A]] = Map[A,collection.mutable.Set[A]]()) 
	{
		import collection.mutable.Set
 
		//checks if a value exists
		def contains(a:A) = data.contains(a);
	 
		//checks if two values are in the same Set
		def sameSet(repr1:A, repr2:A) = data.get(repr1) == data.get(repr2)
	 
		//adds a new Set with one initial value
		def add(a:A) = if (contains(a)) this else new SetMap(data + (a -> Set(a)))
	 
		//adds a new value to an existing Set (specified by repr)
		def insert(a:A, repr:A) = if (contains(a) || ! contains(repr)) this else 
		{
		  val set = data(repr)
		  set += a
		  new SetMap(data + (a -> set))
		}
	 
		//merges two sets (specified by two representants)
		def merge(repr1:A, repr2:A) = if(! contains(repr1) || ! contains(repr2)) this else
		{
			val set1 = data(repr1)
			val set2 = data(repr2)
			if(set1 eq set2) this else 
			{
				set1 ++= set2
				new SetMap(data ++ set2.map(a => a -> set1))
			}
		}
	}
 
	def kruskalBis[A](list : LE[A]) = list.sortBy(_.weight).foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
	
	def kruskalBis[A](list : RDD[EdgeKruskal[A]]) = list.sortBy(_.weight).collect.foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
	 
	def mst[A](t:(LE[A], SetMap[A]), e:EdgeKruskal[A]) = 
	{
		val ((es, sets), EdgeKruskal(p,q,_)) = (t,e)
		(sets.contains(p), sets.contains(q)) match 
		{
			case (false, false) => (e :: es, sets.add(p).insert(q,p))
			case (true, false) => (e :: es, sets.insert(q,p))
			case (false, true) => (e :: es, sets.insert(p,q))
			case (true,true) => if (sets.sameSet(p,q)) (es, sets) //Cycle
								else (e :: es, sets.merge(p,q))
		}
	}


	def kruskalTree(weightMatrix : Array[Array[Double]]) = 
	{
		var nbNodes = weightMatrix.size
		var listEdge = List[Kruskal.EdgeKruskal[Symbol]]()
		for (i <- 0 to nbNodes-2)
		{
			for (j <- i+1 to nbNodes-1) 
			{
				listEdge = listEdge ::: List(EdgeKruskal(Symbol(i.toString),Symbol(j.toString),weightMatrix(i)(j)))
			}
		}
		println(kruskalBis(listEdge))
	}
	
	def kruskalTree(samples : RDD[LabeledPoint], sc : SparkContext) = 
	{
		var nbNodes = samples.count.toInt
		var listEdge = List[Kruskal.EdgeKruskal[Symbol]]()
		for (i <- 0 to nbNodes-2)
		{
			for (j <- i+1 to nbNodes-1) 
			{
				listEdge = listEdge ::: List(EdgeKruskal(Symbol(i.toString),Symbol(j.toString),
				- mutInfo(samples.filter(a => a.label == i.toFloat), samples.filter(a => a.label == j.toFloat))))
			}
		}
		var LEP = sc.parallelize(listEdge)
		println(kruskalBis(LEP))
	}
 
}
