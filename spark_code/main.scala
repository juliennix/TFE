/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// main scala file                                             //
///////////////////////////////////////////////////////////////// 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

import graphicalLearning.Bayesian_network._
import graphicalLearning.ManageFile._
import graphicalLearning.DistributedGraph._
import graphicalLearning.Prims._
import graphicalLearning.Kruskal._
import graphicalLearning.Network._	
import graphicalLearning.Boruvka._

object Main {
    def main(args:Array[String]) = 
    {
		val sc = new SparkContext(new SparkConf())
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
        
        //~ val FileContents = FileReader(filename, labeldelimiter, delimiter)
       
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
        //~ val FileContents = FileMapReader(filename, labeldelimiter, delimiter, sc)
        
        // Retrieve the content in an adequate file in a RDD[LabeledPoint]
		//~ val FileContents = FileRDDReader(filename, labeldelimiter, delimiter, sc)
		
		val FileContents = FileGraphReader(filename, labeldelimiter, delimiter, sc)
		//~ createGraph(FileContents, sc)
		
        //~ var M = skelTree(FileContents)
        //~ kruskalTree(FileContents, sc)
        //~ WriteAdjacency(M)
        //~ PrimsAlgo("PrimsInput", sc)
        var graph = SampleGraph(FileContents, sc).cache
        //~ kruskal(graph)
        //~ PrimsAlgo(graph)
        //~ networkCreation(graph.edges.collect)
        boruvkaAlgo(graph)
        val t2 = System.currentTimeMillis
		// Compute the time (in ms) of the main file
        println((t2 - t1) + " msecs")
    }
}







