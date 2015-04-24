/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perfom kruskal algorithm                                    //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import Array.ofDim
import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag

import graphicalLearning.MutualInfo._

object Kruskal extends Serializable
{
	
 	// Kruskal algortihm in a functional fashion with only the first minimum weight edge and the src/dst key are taken on the local drive.
	def recKruskal(edges : RDD[Edge[Double]], finalE : RDD[Edge[Double]], vertices : RDD[(Long,Long)], length : Int, i : Int = 0) : RDD[Edge[Double]] =
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
	
	def kruskalEdgeRDD[VD : ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double] =
	{
		val nbNodes = graph.vertices.count.toInt
		val setVertices = graph.vertices.map{ case ( id,_)=> (id,id)}
		val setEdges = graph.edges.filter( x => x != x)
		
		return Graph(graph.vertices, recKruskal(graph.edges.sortBy(_.attr), setEdges, setVertices, nbNodes - 1))
	}
 	
 	// Here, we collect the sorted edges in order to loop on those and perform transformation on RDD (as we cannot loop on a RDD and access others)
 	// the way to specify the vertices are part of a growing tree is done on RDD despite the destination set is taken on the local drive
 	def containV(edges : Array[Edge[Double]], setVertices : RDD[Set[Long]], setEdges : Set[Edge[Double]] = Set[Edge[Double]]()) : Set[Edge[Double]] =
	{
		val setSize = setVertices.count
		if (setSize == 1L)	return setEdges
		
		val edge = edges.head
		val src = edge.srcId
		val dst = edge.dstId 
		val setDst = setVertices.filter(set => set.contains(dst)).first
		val newVertices = setVertices.map(set =>	
			if (set.contains(src) && !set.contains(dst)) set ++ setDst
			else set).filter(set => (set.contains(src) && set.contains(dst)) || (!set.contains(src) && !set.contains(dst))) 
		val newSetSize = newVertices.count
		if(newSetSize < setSize)
			containV(edges.drop(1), newVertices, setEdges ++ Set(edge))
		else
			containV(edges.drop(1), newVertices, setEdges)		
	}
	def kruskalEdges[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double] =
	{
		val setVertices = graph.vertices.map{ case (vid, _) => Set(vid)}
		val edges = graph.edges.sortBy(_.attr).collect

		val  setEdges = containV(edges, setVertices)	
		return graph.subgraph(e => setEdges.contains(e), (v,d) => true) 
	} 	
 	

 	// Here, sorting the edge is done on RDD but the result is collected, as the vertices thus, the computation is done mostly on the local drive
	def remove(num: Set[Long], A: Array[Set[Long]]) = A diff Array(num)

	def setOfGoodEdges[VD: ClassTag](graph : Graph[VD, Double]) :  Set[Edge[Double]] =
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
						if (!srcFound && setVertices(i).contains(src)){
							indexSrc = i
							srcFound = true}
						if (!dstFound && setVertices(i).contains(dst)){
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
	
	def kruskalEdgesAndVertices[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double] =
	{
		val setEdges = setOfGoodEdges(graph)
		return graph.subgraph(e => setEdges.contains(e), (v,d) => true)
	}
	
	// code of Daniel Gonau//
	// https://dgronau.wordpress.com/2010/11/28/nochmal-kruskal
	// Compute the mwst on the local drive taking an array of Edge or
	// with RDD[labeledPoint] to sort on RDD then on the local drive
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

	// on the local drive
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
	
	// sorted done on RDD
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
	
    def skelTree(samples : RDD[LabeledPoint]) : Array[Array[Double]] = 
    {
        var nbNodes = samples.count.toInt
        var M = ofDim[Double](nbNodes, nbNodes) 
        
        for (i <- 0 to nbNodes-2)
        {
            for (j <- i+1 to nbNodes-1) 
            {
                M(i)(j) = - mutInfo(nbNodes, samples, i.toLong, j.toLong)
            }
        }
        
        kruskalTree(M)
        return M
        
    }
    
	//~ def skelTree(samples : org.apache.spark.rdd.RDD[(Float, Array[Double])]) : Array[Array[Double]] = 
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
    
    //~ def skelTree(samples : RDD[LabeledPoint]) : Array[Array[Double]] = 
    //~ {
        //~ val nbNodes = samples.count.toInt
        //~ val M = ofDim[Double](nbNodes, nbNodes) 
        //~ 
        //~ for (i <- 0 to nbNodes-2)
        //~ {
            //~ for (j <- i+1 to nbNodes-1) 
            //~ {
//~ 
                //~ M(i)(j) = - mutInfo(samples.filter(a => a.label == i.toFloat), samples.filter(a => a.label == j.toFloat))
            //~ }
        //~ }
        //~ kruskalTree(M)
        //~ return M
        //~ 
    //~ }
 
}
