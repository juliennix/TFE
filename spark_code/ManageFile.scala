/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// File manager                                                //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.io._
import scala.io.Source
import math.random

object ManageFile extends Serializable
{   
	
	def getLineNumber(fileName: String) : Int = 
	{
		scala.io.Source.fromFile(fileName).getLines.size
	}
	// Functions which takes an adequate file and its delimiter between 
	// label and the sample and a delimiter for the sample itself
	// Return an RDD[(Double, Array[Double])] where the FLoat corresponds
	// to the label and the array to samples contained in the file
    def FileToPairRDD(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[(Double, Array[Double])] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            (parts(0).toDouble, parts(1).split("""\""" + delimiter).map(_.toDouble))
        }
        return dataRDD
    }

	// Functions which takes an adequate file and its delimiter between 
	// label and the sample and a delimiter for the sample itself
	// Return an RDD[LabeledPoint] where the label corresponds
	// to the label and the features to samples contained in the file  
    def FileGraphReader(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[LabeledPoint] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD : RDD[LabeledPoint] = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split("""\""" + delimiter).map(_.toDouble)))
        }
        return dataRDD
    }
       
    
    // Function which take a adjacency matrix corresponding to the depedency
    // between variables and create a file in order to be used for the Prims
    // algorithm
    def WriteAdjacency[T](adjacencyMatrix : Array[Array[T]]) =
    {  
        println("Now writing... ")
        var i = -1
        var size = adjacencyMatrix.size
		var str = ""
		val outputFile = new File("PrimsInput")
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		for (i <- 0 to size -1)
		{
			str = i.toString + " "
			for (j <- 0 to size -1)
			{				
				if (i != j) 
				{ 	
					if ( i > j)
						if (i == size - 1 && j == size - 2)
							str = str + j.toString + ":" + adjacencyMatrix(j)(i).toString
						else
							str = str + j.toString + ":" + adjacencyMatrix(j)(i).toString + ","
					else if (j == size -1)
						str = str + j.toString + ":" + adjacencyMatrix(i)(j).toString 
					//~ else if (i == size -1 && j == size - 2)
						//~ str = str + j.toString + ":" + adjacencyMatrix(i)(j).toString 
					else 
						str = str + j.toString + ":" + adjacencyMatrix(i)(j).toString + ","
				}
			}
			writer.write(str)
			writer.write("\n")
		}
		writer.close()
	}

	// Function which takes a number of rows and the length of them
	// Write a file "test" containing 0-1 values corresponding to a 
	// file test that could be used to proceed performance test
	def writeExample(nbSample : Int, sampleLength : Int, fileName : String, from : Int = 0, to : Int = 1) = {
		println("Now writing... ")
		var str = ""
		val outputFile = new File(fileName)
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		for (line <- 0 to nbSample - 1)
		{
			str = line.toString + 	","
			for (element <- 0 to sampleLength - 1)
			{				 
				if (element == sampleLength - 1)
					str = str + ((random * (to - from)) + from).round.toString
				else 
					str = str + ((random * (to - from)) + from).round.toString + " "
			}
			writer.write(str)
			if (line != nbSample - 1)
				writer.write("\n")
		}
		writer.close()
	}
	
	def intList(l : List[String]) = l.map(x=>Integer.parseInt(x))
}
