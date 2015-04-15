/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Prim's algorithm                                            //
///////////////////////////////////////////////////////////////// 


package graphicalLearning

import scala.util.control.Breaks._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import scala.reflect.ClassTag
//~ import scalaz._ 


object Prims extends Serializable
{
	
	// This version is done in a functional fashion only through RDD. Could create also a keys-values RDD and filter it
	// in order to not take all the edges att each step
	def rPrim( nbNodes : Int, iter : Int = 0,  finalE : RDD[Edge[Double]], joinedVE : RDD[(Int, (Set[Long], Edge[Double]))]) : RDD[Edge[Double]] = 
	{
		if ( iter == nbNodes - 1) return finalE
		else {
			val min = joinedVE.filter{ case (key, (set, edge)) => (set.contains(edge.srcId) && !set.contains(edge.dstId)) ||
								  (!set.contains(edge.srcId) && set.contains(edge.dstId))}.reduceByKey{ case ((set1,edge1), (set2,edge2)) => 
								  { if (edge1.attr < edge2.attr) (set1,edge1) else (set2,edge2)}}
								  
			val edge = min.map{ case (key, (set, edge)) => edge}
			val newJoined = joinedVE.join(min).map{ case ( key, ((set , edge), (set2, newEdge))) => (key, (set ++ Set(newEdge.srcId) ++ Set(newEdge.dstId), edge))}
			rPrim(nbNodes, iter +1, finalE ++ edge, newJoined)
		}
	} 	
	def PrimsRDD[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double]  = 
	{
		val nbNodes = graph.vertices.count.toInt
		val label = graph.vertices.first._1 
		val keyV = graph.vertices.filter(x => x._1 == label).map(x => x._1).map(x => (1,Set(x)))
		val keyE = graph.edges.map(x => (1, x))
		val joinedVE = keyV.join(keyE) 
		val finalE = graph.edges.filter(e => e != e)
		return Graph(graph.vertices, rPrim(nbNodes, 0, finalE, joinedVE))	
	}
	
	// This version is done in a functional fashion but the edge at each step still taken on the local drive. Could create also a keys-values RDD and filter it
	// in order to not take all the edges att each step
	def recPrim(nbNodes : Int, iter : Int = 0,  finalE : RDD[Edge[Double]], keyV : RDD[(Int, Set[Long])], keyE : RDD[(Int, Edge[Double])]) : RDD[Edge[Double]] = 
	{
		
		if ( iter == nbNodes - 1) finalE
		else {
			val joinedVE = keyV.join(keyE).map{ case (key, value) => value}
			val edge = joinedVE.filter{ case (set, edge) => (set.contains(edge.srcId) && !set.contains(edge.dstId)) ||
								  (!set.contains(edge.srcId) && set.contains(edge.dstId))}.reduceByKey((v1,v2) => 
								  { if (v1.attr < v2.attr) v1 else v2}).map{ case (set, edge) => edge}
								 
			val src = edge.first.srcId
			val dst = edge.first.dstId
			val newKV = keyV.map(x => (x._1, x._2 ++ Set(src) ++ Set(dst)))
			recPrim(nbNodes, iter +1, finalE ++ edge, newKV, keyE)
		}
	} 	
	def PrimsDistFuncEdge[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double]  = 
	{
		val nbNodes = graph.vertices.count.toInt
		val label = graph.pickRandomVertex()
		val keyV = graph.vertices.filter(x => x._1 == label).map(x => x._1).map(x => (1,Set(x)))
		val keyE = graph.edges.map(x => (1, x))
		val finalE = graph.edges.filter(e => e != e)
		return Graph(graph.vertices, recPrim(nbNodes, 0, finalE, keyV, keyE)) 	
	}

	// The only element on the local drive here is the edge computed thanks to the min method on an RDD
	// keys-values are used with join/filter... methods to construct RDD containing the correct edges
	def PrimsEdge[VD: ClassTag](graph : Graph[VD, Double]) : Graph[VD, Double]  = 
	{
		val nbNodes = graph.vertices.count.toInt
		val label = graph.pickRandomVertex()
		// Initiate RDD with key/variable
		var keyV = graph.vertices.filter(x => x._1 == label).map(x => x._1).map(x => (1,Set(x)))
		val keyE = graph.edges.map(x => (1, x))
		//~ val keyE = graph.edges.map(x => if (x.srcId == label || x.dstId == label)(0, x) else (1, x))
		// Join edges and vertices and remove the key
		val joinedVE = keyV.join(keyE).map(a => a._2)
		//~ val onlyGoodE = keyE.subtractByKey(keyV)
		//~ val joinedVE = keyV.join(keyE).map(a => a._2)
		// Find the minimum Edge
		val edge = joinedVE.filter(VE => (VE._1.contains(VE._2.srcId) && !VE._1.contains(VE._2.dstId)) ||
								  (!VE._1.contains(VE._2.srcId) && VE._1.contains(VE._2.dstId))).min()(Ordering.by(e => e._2.attr))._2
		// Filter in order to retrieve the min edgeRDD
		var finalE = joinedVE.filter(VE => VE._2 == edge).map(x => x._2).cache
		//~ 
		val src = edge.srcId
		val dst = edge.dstId
		// Could change the key in order not to check edges already taken
		//~ keyV.foreach(v => println(v))
		keyV = keyV.map(x => (x._1, x._2 ++ Set(src) ++ Set(dst)))
		//~ keyV.foreach(v => println(v))
		//~ 
		for (i <- 0 to nbNodes - 3)
		{
			val joinedVE = keyV.join(keyE).map(a => a._2)
			// Find the minimum Edge
			val edge = joinedVE.filter(VE => (VE._1.contains(VE._2.srcId) && !VE._1.contains(VE._2.dstId)) ||
								  (!VE._1.contains(VE._2.srcId) && VE._1.contains(VE._2.dstId))).min()(Ordering.by(e => e._2.attr))._2
								 
			finalE = (finalE ++ joinedVE.filter(VE => VE._2 == edge).map(x => x._2)).cache
			//~ println(edge)
			//~ edgeRDD.foreach(println)
			//~ finalE.foreach(println)
			val src = edge.srcId
			val dst = edge.dstId
			//~ keyV.foreach(v => println(v))
			keyV = keyV.map(x => (x._1, x._2 ++ Set(src) ++ Set(dst)))
			//~ keyV.foreach(v => println(v))
		}
		return Graph(graph.vertices, finalE)	
	}

	// The edges and vertices are in the local driver
	def PrimsAlgo[VD: ClassTag](graph : Graph[VD, Double])  : Graph[VD, Double] = 
	{
		var setVertices = Set[Long](graph.vertices.first._1)
		var setEdges = Set[Edge[Double]]()
		
		// A fibonacci heap is usually used in sequential Prim's algo
		// No distributed version exists and it is pointless since the 
		// minimum function here is done on an rdd actually
		//~ var h = Heap[(VertexId, Node)]()
		
		for (i <- 0 to graph.vertices.count.toInt - 2)
		{
			// could try on a total graph and using other way to produce a "growing tree"
			var edge = graph.subgraph(e => (setVertices.contains(e.srcId) && !setVertices.contains(e.dstId))
											|| (!setVertices.contains(e.srcId) && setVertices.contains(e.dstId))
											//&& !setEdges.contains(e), (v,d) => true
											).edges.min()(Ordering.by(e => e.attr))

			setVertices += edge.srcId 
			setVertices += edge.dstId 
			setEdges += edge
		}
        return graph.subgraph(e => setEdges.contains(e), (v,d) => true)
	}

	// Algorithm from a discusion on a google forum https://groups.google.com/forum/#!topic/spark-users/KBWyiNDl-kY
	// It uses a file represents a graph, line by line there is the the source and then then destination with the associated weight in order to compute the mwst.
	// It returns a String with separator containing the chosen edges
	def PrimsMap(row: String): List[(String,String)] = 
	{    
		val nodeId = row.split(" ")(0)
		val edge = row.split(" ")(1)
		var newEdge = ""
		var mapOutput = List[(String, String)]()
		
		if (nodeId == "-1") mapOutput = (nodeId, edge) :: mapOutput
		//test the inputs
		else if (edge != "" && edge != ",") 
		{
			val edgeList = edge.split(",").sortBy(x => x.split(":")(1).toDouble)
			mapOutput = ("-1", nodeId + "=>" + edgeList(0)) :: mapOutput
			if (edgeList.length > 1) newEdge = edgeList.filter(_.contains(edgeList(0)) == false).reduce(_ + "," + _)
			mapOutput = (nodeId, newEdge) :: mapOutput          
		}
		return mapOutput
	}    

	def PrimsReduce(rows: Seq[String]): List[String] = 
	{
		var mstEdge = List[String]()
		var mstWeight = 0.0
		var weight = 0.0
		var reduceOutput = List[String]()

	    if (rows.toString().contains("=>") == false) reduceOutput = rows(0) :: reduceOutput
	    else 
	    {
			val mstList = rows.filter(_.contains("mst"))
			val edgeList = rows.filter(_.contains("=>")).sortBy(x => x.split(":")(1))
			if (mstList.length != 0) 
			{
				mstEdge = mstList.filter(_.contains("mstEdge"))(0).split("_")(1).split(",").toList
				mstWeight = mstList.filter(_.contains("mstWeight"))(0).split("_")(1).toDouble
			}
       
			breakable 
			{
				for (i <- 0 to (edgeList.length - 1)) 
				{
					if (mstEdge.length == (edgeList.length - 1)) break 
				       
					val srcNode = edgeList(i).split("=>")(0)
					val dstNode = edgeList(i).split("=>")(1).split(":")(0)
					weight = edgeList(i).split("=>")(1).split(":")(1).toDouble
				
					if (mstEdge.map(x => x.split("-")(0)).contains(dstNode) == false) 
					{
						mstEdge = srcNode + "-" + dstNode :: mstEdge
						mstWeight += weight
					}                            
				}      
			}      
			reduceOutput = "mstEdge_" + mstEdge.reduce(_ + "," + _) :: reduceOutput
			reduceOutput = "mstWeight_" + mstWeight.toString() :: reduceOutput
	    }
		return reduceOutput    
	}  

	def PrimsAdjAlgo(filename : String, sc : SparkContext) 
	{
		var iterativeInput = sc.textFile(filename)

	    val totalNode = iterativeInput.count().toInt
	    var totalEdge = 0
	    var iteration = 0       

	    while (totalEdge < (totalNode - 1)) 
	    {
			val mapResult = iterativeInput.flatMap(x => PrimsMap(x)).groupByKey().cache()
			val reduceResult = mapResult.mapValues(x => PrimsReduce(x.toSeq)).cache()
			iterativeInput = reduceResult.flatMap { case (k, vs) => vs.map(v => (k+" "+ v)) }.cache()
			totalEdge = iterativeInput.filter(_.contains("mstEdge")).flatMap(x => x.split(" ")(1).split("_")(1).split(",")).count().toInt
			iteration += 1
		}    
		val MSTEdge = iterativeInput.filter(_.contains("mstEdge")).map(x => x.split(" ")(1))
		val MSTWeight = iterativeInput.filter(_.contains("mstWeight")).map(x => x.split(" ")(1))
		val output = MSTEdge.union(MSTWeight)
		output.collect.map(println)
		System.out.println("Total Number of Iterations: " + iteration.toString())
	}
}
