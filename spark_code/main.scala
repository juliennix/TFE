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
import graphicalLearning.TestMethod._

object Main {
    def main(args:Array[String]) = 
    {
		// SPARK CONFIGURATION //
		
		// Set the log messages of spark off
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF) 
		
		// Define the spark context
		val conf = new SparkConf()
             .setMaster("local[1]")
             .setAppName("bayesian_network")
             .set("spark.executor.memory", "1g")
        val sc = new SparkContext(conf)
        
        // INPUT VARIABLES //
        
        print("Please enter your name's textfile : " )
        //~ val filename = Console.readLine
        //~ val filename = "simple_labeled"
        val filename = "test_file/2_values/50nodes"
        val filenameTrain = "test_file/data/data_observations_number/data_120/DAG__num_var200_m_par5_0_samples120_data0.dat"
        //~ val filenameTest = "test_file/200_5/DAG__num_var200_m_par5_0_samples50000_validation.dat"
        val filenameTest = "test_file/data/data_observations_number/test0.dat"
        val filenameValidation = "test_file/data/data_observations_number/validation0.dat"
        
        print("Please enter your label delimiter in this file : " )
        //~ val labeldelimiter = Console.readLine
        val labeldelimiter = ","
        val varDelimiter = " "   
            
        print("Please enter your delimiter in this file : " )
        //~ val delimiter = Console.readLine
        val obsDelimiter = "; "

        // READ INPUT FILES //
        
        // Retrieve the content in an adequate file in a RDD[Double, Array[Double]]
        //~ val variablesSample = FileToPairRDDVar(filename, labeldelimiter, delimiter, sc)
        // Retrieve the content in an adequate file in a RDD[Double, Array[Double]]
        val train = FileToRDDObs(filenameTrain, obsDelimiter, sc)
        val test = FileToRDDIndexedObs(filenameTest, labeldelimiter, obsDelimiter, sc)
        val variablesSample = getVariableSampleFromObs(train)
        val evidenceSetRDD = getEvidenceFromTest(test.map{case(id, arr)=> arr})
		val validation = FileToRDDIndexedValidation(filenameValidation, labeldelimiter, sc)
        
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
		val labeledContent = FileGraphReader(filename, labeldelimiter, varDelimiter, sc)
		
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
        
        val graph4 = RDDFastGraph(variablesSample,sc)
        
        // graph functions to define fully dense graphs
        val fullGraph1 = LabeledfastFullGraph(labeledContent, sc).cache
        
        val fullGraph2 = RDDfastFullGraph(variablesSample, sc).cache

		// MWST ALGORITHMS (first) //
		// Those algorithm compute the mwst partially on the local driver
		// see comment of those functions to know the degree of distribution
		
		// Kruskal
        val kruskalGraph = kruskalEdgesAndVertices(graph4)
                
        // Prim
        val primGraph = PrimsAlgo(graph4)
        
        // Boruvka
        val boruvkaGraph = boruvkaAlgo(graph4)


        // MWST ALGORITHMS (second) //
        
        // Kruskal
        val kruskalGraph2 = kruskalEdges(graph4)
        val kruskalGraph3 = kruskalEdgeRDD(graph4)
        
        // Prim
        val primGraph2 = PrimsDistFuncEdge(graph4)
        val primGraph3 = PrimsEdge(graph4)
        val primGraph4 = PrimsRDD(graph4)
        
        // Boruvka
        val boruvkaGraph2 = boruvkaDistAlgo(graph4)
                
        // GHS
        val messageGraph = GHSGraph(variablesSample, sc)
        val GHSMwstGraph = GHSMwst(messageGraph)
        
        // DISPLAY GRAPHS //
        
        networkCreation(graph4.edges.collect)
        
        // MARKOVTREE CREATION // 

        val markovTree = markovTreeCreation(GHSMwstGraph)
        val markovTreeSetUp = learnParameters(markovTree, variablesSample)
        
        // DISPLAY TREE //
        
        val root = markovTree.vertices.filter{ case (key, markovNode) => markovNode.level == 0}.first._1
        treeCreation(markovTree.edges.collect, root.toString)
        
        // INFERENCE
        
        val evidence = EvidenceSet()	
        //~ val evidence = evidenceSetRDD.first	
        val inferedMarkovTree = inference(markovTreeSetUp, evidence)
        
        // MIXTURE TREE BY BOOTSTRAPING 
        val t1 = System.currentTimeMillis
        val numberOfTree = 5
        val mixtureTree = createMixtureWithBootstrap(sc, variablesSample, numberOfTree)
        val mixtureTreeBayes = createMixtureWithBootstrapBayes(sc, variablesSample, numberOfTree)
        val t2 = System.currentTimeMillis
        val inferedProb = getInferedProbability(mixtureTree, evidence)
        val t3 = System.currentTimeMillis
		println((t2 - t1)/1000D, (t3 - t2)/1000D)
     
		//~ val (train, test) = getTrainAndTestSet(variablesSample)
		
		val trainGraph = GHSGraph(variablesSample, sc)
        val GHSMwstGraph2 = GHSMwst(trainGraph)

        // INFERENCE ON THE MIXTURE (INFERENCE PER TREE AND THEN AVERAGING)
        val evidencePerObs = evidenceSetRDD.zipWithIndex
        for (i <- 0 to (evidenceSetRDD.count.toInt-1)) yield (getInferedProbability(mixtureTree, evidencePerObs.filter(e => e._2 == i.toLong).first._1))
		
		//~ val fraction = 5
		//~ EM(sc, variablesSample, fraction)

        // TEST 
                
        val fileNames = Array[String]("test_file/2_values/5nodes", "test_file/2_values/10nodes", "test_file/2_values/20nodes",
        "test_file/2_values/30nodes", "test_file/2_values/40nodes", "test_file/2_values/50nodes", "test_file/2_values/60nodes", "test_file/2_values/70nodes", "test_file/2_values/80nodes", "test_file/2_values/90nodes",
        "test_file/2_values/100nodes", "test_file/2_values/125nodes", "test_file/2_values/150nodes", "test_file/2_values/200nodes")
        //~ "test_file/2_values/125nodes", "test_file/2_values/150nodes", "test_file/2_values/200nodes", "test_file/2_values/350nodes", "test_file/2_values/500nodes")
        val repeat = 3
        //~ val fileNames = Array[String]("test_file/2_values/500nodes")
        //~ val method = Array[Int](1,3,5,6,7,8,10)
        val method = Array[Int](1, 3, 4, 10)
        computationTimeChart(fileNames, repeat, method, sc)
        
        val numberOfSample = train.count.toInt
        val score = KLDivergenceRDD(mixtureTree, test, validation, numberOfSample, sc)


        KLDivergenceRDD(mixtureTree, test, validation, numberOfSample, sc)
        KLDivergenceChart(variablesSample, test, validation, 1, 6, 2, sc)
        
        
    }
}







