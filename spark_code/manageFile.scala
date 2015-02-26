/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// File manager                                                //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.io._
import math._

object ManageFile 
{   
	// Functions which takes an adequate file and its delimiter between 
	// label and the sample and a delimiter for the sample itself
	// Return an RDD[(Float, Array[Float])] where the FLoat corresponds
	// to the label and the array to samples contained in the file
    def FileMapReader(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[(Float, Array[Float])] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            (parts(0).toFloat, parts(1).split("""\""" + delimiter).map(_.toFloat))
        }
        return dataRDD
    }

	// Functions which takes an adequate file and its delimiter between 
	// label and the sample and a delimiter for the sample itself
	// Return an RDD[LabeledPoint] where the label corresponds
	// to the label and the features to samples contained in the file 
    def FileRDDReader(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[LabeledPoint] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD : RDD[LabeledPoint] = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            LabeledPoint(parts(0).toFloat, Vectors.dense(parts(1).split("""\""" + delimiter).map(_.toDouble)))
        }
        return dataRDD
    }  
    
    def FileGraphReader(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[LabeledPoint] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD : RDD[LabeledPoint] = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            LabeledPoint(parts(0).toLong, Vectors.dense(parts(1).split("""\""" + delimiter).map(_.toDouble)))
        }
        return dataRDD
    }  
    
    // Function which take a adjacency matrix corresponding to the depedency
    // between variables and create a file in order to be used for the Prims
    // algorithm
    def WriteAdjacency(adjacencyMatrix : Array[Array[Float]]) =
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
				{ 	if ( i > j)
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
	def writeExample(nbSample : Int, sampleLength : Int) = {
		println("Now writing... ")
		var str = ""
		val outputFile = new File("test")
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		for (line <- 0 to nbSample)
		{
			str = line.toString + 	","
			for (element <- 0 to sampleLength)
			{				 
				if (element == sampleLength)
					str = str + random.round.toString 
				else 
					str = str + random.round.toString + " "
			}
			writer.write(str)
			if (line != nbSample)
				writer.write("\n")
		}
		writer.close()
	}
	
	def intList(l : List[String]) = l.map(x=>Integer.parseInt(x))
}
