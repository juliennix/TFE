/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// main scala file                                             //
///////////////////////////////////////////////////////////////// 

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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
import graphicalLearning.MarkovTree._
import graphicalLearning.Inference._
import graphicalLearning.EvidenceSet
import graphicalLearning.MixtureTree._
import graphicalLearning.Resample._
import graphicalLearning.Plot._

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
        val filename = "10nodes"
        
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
		// Those algorithm compute the mwst partially on the local driver
		// see comment of those functions to know the degree of distribution
		
		// Kruskal
        val kruskalGraph = kruskalEdgesAndVertices(graph4)
        val kruskalGraph2 = kruskalEdges(graph4)
                
        // Prim
        val primGraph = PrimsAlgo(graph4)
        
        // Boruvka
        val boruvkaGraph = boruvkaAlgo(graph4)


        // MWST ALGORITHMS (second) //
        
        // Kruskal
        val kruskalGraph3 = kruskalEdgeRDD(graph4)
        
        // Prim
        val primGraph2 = PrimsDistFuncEdge(graph4)
        val primGraph3 = PrimsEdge(graph4)
        val primGraph4 = PrimsRDD(graph4)
        
        // Boruvka
        val boruvkaGraph2 = boruvkaDistAlgo(graph4)
                
        // GHS
        val messageGraph = GHSGraph(content, sc)
        val GHSMwstGraph = GHSMwst(messageGraph)
        
        // DISPLAY GRAPHS //
        
        networkCreation(graph4.edges.collect)
        
        // MARKOVTREE CREATION // 

        val markovTree = markovTreeCreation(GHSMwstGraph)
        val markovTreeSetUp = learnParameters(markovTree, content)
        
        // DISPLAY TREE //
        
        val root = markovTree.vertices.filter{ case (key, markovNode) => markovNode.level == 0}.first._1
        treeCreation(markovTree.edges.collect, root.toString)
        
        // INFERENCE
        
        val evidence = EvidenceSet()
        val inferedMarkovTree = inference(markovTreeSetUp, evidence)
        
        // MIXTURE TREE BY BOOTSTRAPING 
        
        val numberOfTree = 5
        val mixtureTree = createMixtureWithBootstrap(sc, content, numberOfTree)
        val inferedMixture = getInferedProbability(mixtureTree, evidence, numberOfTree)
     
		val (train, test) = getTrainAndTestSet(content)
		
		val trainGraph = GHSGraph(train, sc)
        val GHSMwstGraph2 = GHSMwst(trainGraph)

        // INFERENCE ON THE MIXTURE (INFERENCE PER TREE AND THEN AVERAGING)
        
        // TEST 
        val fileNames = Array[String]("5nodes", "10nodes", "20nodes", "50nodes", "100nodes", "200nodes", "350nodes", "500nodes")
        val repeat = 1
        val method = Array[Int](1,3,5,6,7,8,10)
        computationTimeChart(fileNames, repeat, method, sc)
        
        // TIME COMPUTATION //
        val t2 = System.currentTimeMillis
		// Compute the time (in ms) of the main file
        println((t2 - t1) + " msecs")
    }
}







