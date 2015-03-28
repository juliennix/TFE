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
		// SPARK CONFIGURATION //
		
		// Set the log messages of spark off
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF) 
		
		// Define the spark context
		val conf = new SparkConf()
             .setMaster("local[4]")
             .setAppName("bayesian_network")
             .set("spark.executor.memory", "2g")
        val sc = new SparkContext(conf)
        
        // INPUT VARIABLES //
        
        print("Please enter your name's textfile : " )
        //~ val filename = Console.readLine
        //~ val filename = "simple_labeled"
        val filename = "simple_labeled"
        
        print("Please enter your label delimiter in this file : " )
        //~ val labeldelimiter = Console.readLine
        val labeldelimiter = ","
        
        print("Please enter your delimiter in this file : " )
        //~ val delimiter = Console.readLine
        val delimiter = " "
        
        val t1 = System.currentTimeMillis
        
        // READ INPUT FILES //
        
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
        val content = FileToPairRDD(filename, labeldelimiter, delimiter, sc)
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
		val labeledContent = FileGraphReader(filename, labeldelimiter, delimiter, sc)
		
		// Some old way to compute the mwst from a mutual information' array
        val M = skelTree(labeledContent)
		val graph = MatrixToGraph(M, sc)
        kruskalTree(labeledContent, sc)
        WriteAdjacency(M)
        PrimsAdjAlgo("PrimsInput", sc)
        
        // CREATE GRAPHS //
        
        // these functions are here to prove that the computation depends a lot on
        // the way you code, better use the last one. Note that the graphs are
        // directed graph and not fully dense
        val graph1 = directSlowInfoGraph(labeledContent, sc).cache
        
        val graph2 = afterInfoGraph(labeledContent, sc).cache
        
        val graph3 = LabeledFastGraph(labeledContent, sc).cache
        
        val graph4 = RDDFastGraph(content,sc)
        
        // graph functions to define fully dense graphs
        val fullGraph1 = LabeledfastFullGraph(labeledContent, sc).cache
        
        val fullGraph2 = RDDfastFullGraph(content, sc).cache

		// MWST ALGORITHMS (first) //
		// Could return graphs directly indeed
		
		// Kruskal
        val kruskalSetEdges = kruskal(graph4)
        val kruskalGraph = graph4.subgraph(e => kruskalSetEdges.contains(e), (v,d) => true)
        
        // Prim
        val primSetEdges = PrimsAlgo(graph4)
        val primGraph = graph4.subgraph(e => primSetEdges.contains(e), (v,d) => true)
        
        // Boruvka
        val boruvkaSetEdges = boruvkaAlgo(graph4)
        val boruvkaGraph = graph4.subgraph(e => boruvkaSetEdges.contains(e), (v,d) => true)

        // MWST ALGORITHMS (second) //
        
        // Kruskal
        val kruskalEdgeRDD = kruskalDist(graph4)
        val kruskalGraph2 = Graph(graph4.vertices, kruskalEdgeRDD)
        
        // Prim
        val primEdgeRDD = PrimsRDD(graph4)
        val primGraph2 = Graph(graph4.vertices, primEdgeRDD)
        
        // Boruvka
        val boruvkaEdgeRDD = boruvkaDistAlgo(graph4)
        val boruvkaGraph2 = Graph(graph4.vertices, boruvkaEdgeRDD)
                
        // GHS
        val MessageGraph = GHSGraph(content, sc)
        val GHSMwstGraph = GHSmst(MessageGraph)
        
        // DISPLAY GRAPHS //
        
        networkCreation(graph4.edges.collect)
        
        // MARKOVTREE CREATION // 

        val markovTree = orientedGraph(kruskalGraph2)
        val markovTreeSetUp = setUpMarkovTree(markovTree, content)
        // TIME COMPUTATION //
        
        val t2 = System.currentTimeMillis
		// Compute the time (in ms) of the main file
        println((t2 - t1) + " msecs")
    }
}







