/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// main scala file                                             //
///////////////////////////////////////////////////////////////// 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import graphicalLearning.MutualInfo._
import graphicalLearning.ManageFile._
import graphicalLearning.DistributedGraph._
import graphicalLearning.Prims._
import graphicalLearning.Kruskal._
import graphicalLearning.Network._	
import graphicalLearning.Boruvka._
import graphicalLearning.GHS._
import graphicalLearning.Inference._


object Main {
    def main(args:Array[String]) = 
    {
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF) 
		val conf = new SparkConf()
             .setMaster("local[4]")
             .setAppName("bayesian_network")
             .set("spark.executor.memory", "2g")
             
		val sc = new SparkContext(conf)
        print("Please enter your name's textfile : " )
        //~ val filename = Console.readLine
        //~ val filename = "simple_labeled"
        val filename = "test"
        
        
        print("Please enter your label delimiter in this file : " )
        //~ val labeldelimiter = Console.readLine
        val labeldelimiter = ","
        
        print("Please enter your delimiter in this file : " )
        //~ val delimiter = Console.readLine
        val delimiter = " "
        
        val t1 = System.currentTimeMillis
        
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
        val cnt = FileToPairRDD(filename, labeldelimiter, delimiter, sc)
        val graph = fastGraphSecond(cnt,sc)
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
		//~ val FileContents = FileGraphReader(filename, labeldelimiter, delimiter, sc)
		
        //~ var M = skelTree(FileContents)
        //~ kruskalTree(FileContents, sc)
        //~ WriteAdjacency(M)
        //~ PrimsAdjAlgo("PrimsInput", sc)
        
        // this method is here to prove that the computation depends a lot on
        // the way you code, better use the second one
        //~ var graph1 = directSlowInfoGraph(FileContents, sc).cache
        
        //~ var graph2 = afterInfoGraph(FileContents, sc).cache
        
        //~ var graph3 = fastGraph(FileContents, sc).cache
        
        //~ var fullGraph = fastFullGraph(FileContents, sc).cache
        //~ fullGraph.edges.collect
        //~ var setEdges = kruskal(graph3)
        //~ var graph4 = graph3.subgraph(e => setEdges.contains(e), (v,d) => true)
        //~ var setEdges = PrimsAlgo(graph3)
        //~ var graph5 = graph2.subgraph(e => setEdges.contains(e), (v,d) => true)
        //~ var setEdges = boruvkaDistAlgo(fullGraph)
        //~ var graph6 = graph2.subgraph(e => setEdges.contains(e), (v,d) => true)
        //~ var fullGraph1 = fullGraph(FileContents, sc)

        
        //~ networkCreation(graph4.edges.collect)
        
        //~ var graph3 = GHSGraph(FileContents, sc)
        //~ val edgeRDD = PrimsDistAlgoRec(graph)
        val edgeRDD = kruskalDist(graph)
        val graph4 = Graph(graph.vertices, edgeRDD)
        //~ var setEdges = kruskal(graph)
        //~ var graph4 : Graph[Double, Double] = graph.subgraph(e => setEdges.contains(e), (v,d) => true)
        //~ PregelMST(graph4)
        orientGraph(graph4)
		
        
        val t2 = System.currentTimeMillis
		// Compute the time (in ms) of the main file
        println((t2 - t1) + " msecs")
    }
}







