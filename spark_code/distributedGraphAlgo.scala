/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Manages the creation and techniques on distr. graph         //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import graphicalLearning.Bayesian_network._

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
	//~ var queue ==> a priority q
	
	class Node(values : Array[Double]) extends java.io.Serializable
	{
			
		var level = 0
		var sample = values
		var Find_count = 0
		var fragm = None
		var state = "SLEEPING"
		var best_edge = None
		var best_wt = 0
		var test_edge = None
		var phase = None
		var state_edge = "BASIC"
		 //~ # misc:
		//~ var expect_core_report = None
		//~ var report_over = false
		//~ var other_core_node = None
		//~ var discovery = None
		//~ var waiting_to_connect_to = None
		//~ var connect_reqs = ConnectRequests()
		//~ var find_count = None # hmmm
		//~ var find_source = None
		//~ var test_reqs = set()
		//~ var min_wt_outgoing = (None, INFINITY)
		//~ # Used by the Test functions:
		//~ var best_wt = INFINITY
		//~ var best_path = None
		//~ var test_over = None
		//~ # Special variable to indicate when to terminate:
		//~ var finished = False
		
	}
	
	//~ def createGraph(weightMatrix : Array[Array[Float]], sc : SparkContext) :  Graph[Node, Double] = 
	//~ {
        //~ var nbNodes = weightMatrix.size
        //~ 
        //~ var verticesArray = new Array[(Long,Node)](nbNodes)
        //~ var edgesArray = new Array[Edge[Double]](nbNodes*nbNodes)
			//~ for (i <- 0 to nbNodes-2)
			//~ {
				//~ verticesArray(i) = (i.toLong, new Node())
				//~ for (j <- i+1 to nbNodes-1) 
				//~ {
					//~ edgesArray(i+j) = Edge(i.toLong, j.toLong, weightMatrix(i)(j))
				//~ }
			//~ }
        //~ 
        //~ val vertices: RDD[(VertexId,Node)] = sc.parallelize(verticesArray)
        //~ val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(x => x != null))
		//~ val graph = Graph(vertices, edges)
		//~ return graph
	//~ }
	
	def SampleGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Node, Float] = 
	{
        var nbNodes = samples.count.toInt
        var weight = 0
        
        val vertices: RDD[(Long,Node)] = samples.map {x =>
			(x.label.toLong, new Node(x.features.toArray))}
			
        var edgesArray = new Array[Edge[Float]]((nbNodes*(nbNodes - 1 ))/2)
		for (i <- 0 to nbNodes-2)
		{
			for (j <- i+1 to nbNodes-1) 
			{
				edgesArray((i * nbNodes) - ((i+1)*(i+1) - (i+1))/2 + j - (i + 1)) = Edge(i.toLong, j.toLong, weight)
			}
		}
        // if mutinfo is too near from 0 or even == 0 -> no edge ? So should we filter the edgeArray ?
        val edges: RDD[Edge[Float]] = sc.parallelize(edgesArray.filter(x => x != null))
 
		var graph = Graph(vertices,edges)

		//~ //parallel ?
		graph = graph.mapTriplets( triplet => computeMutInfo(triplet))
		
		graph.triplets.min()(Ordering.by(e => e.attr)) 
		
		//~ var msgDst = graph.triplets.filter(x => x.srcId == 0L).min()(Ordering.by(e => e.attr)).dstId
		
		//~ // define the changement of the vertex based on the message it has received 
		
		//~ def sendMessage(edge: EdgeTriplet[Node, Float]) =
//~ 
		//~ {
			//~ 
		//~ }
		//~ 
		
		//~ // define the way message, ie triplets, are send from a node to one or more
		
		//~ def sendMessage(edge: EdgeTriplet[Node, Float]) =
			//~ Iterator((edge.dstId, edge.srcAttr))
			
		//~ // define the way which message the node will take, dont know if we define here the way the nodes
		//~ // are going to choose the right message or not
			
		//~ def messageCombiner(a: Int, b: Int): Int = a + b
		//~ 
		    //~ graph.pregel(0, 2,EdgeDirection.Either)(
      //~ vertexProgram, sendMessage, messageCombiner)

		
		//~ graph.vertices.collect.map(x => println(x._2.state))
		//~ graph.triplets.collect.map(x => x.attr)
		
		//~ graph.mapVertices(x => wakeUp(x))

		return graph
	}
	

	
	val computeMutInfo = (triplet : EdgeTriplet[Node, Float]) =>
			- mutInfo(triplet.srcAttr.sample, triplet.dstAttr.sample)
	
	//~ def createGraph(samples : RDD[LabeledPoint], sc : SparkContext) :  Graph[Node, Double] = 
	//~ {
        //~ var nbNodes = samples.count.toInt
        //~ 
        //~ val vertices: RDD[(Long,Node)] = samples.map {x =>
			//~ (x.label.toLong, new Node())}
			//~ 
        //~ var edgesArray = new Array[Edge[Double]]((nbNodes*(nbNodes - 1 ))/2)
		//~ for (srcVertex <- 0 to nbNodes-2)
		//~ {
			//~ for (dstVertex <- srcVertex+1 to nbNodes-1) 
			//~ {
				//~//Sequentiel :/
				//~ var variable = samples.filter(a => a.label == srcVertex.toFloat)
				//~ var condition = samples.filter(a => a.label == dstVertex.toFloat)
				//~ var mutualInfo = - mutInfo(variable, condition)
				//~ if ( mutualInfo != 0)
					//~ edgesArray(i+j-1) = Edge(i.toLong, j.toLong, mutualInfo)
			//~ }
		//~ }
        //~ // if mutinfo is too low -> no edge ?
        //~ val edges: RDD[Edge[Double]] = sc.parallelize(edgesArray.filter(x => x != null))
 //~ 
		//~ val graph = Graph(vertices, edges)
		//~ return graph
	//~ }
}


//~ val oldestFollower: VertexRDD[(String, Int)] = userGraph.aggregateMessages[(String, Int)]
//~ (
    //~ // For each edge send a message to the destination vertex with the attribute of the source vertex
    //~ sendMsg = { triplet => triplet.sendToDst(triplet.srcAttr.name, triplet.srcAttr.age) },
   //~ // To combine messages take the message for the older follower
    //~ mergeMsg = {(a, b) => if (a._2 > b._2) a else b}
