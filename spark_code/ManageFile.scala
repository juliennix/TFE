/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// File manager                                                //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import java.io._
import scala.io.Source
import math.random
import scala.reflect.ClassTag

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
    def FileToPairRDDVar(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[(Double, Array[Double])] =
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
    
    def FileToRDDObs(absPath:String, delimiter:String, sc : SparkContext): RDD[Array[Double]] =
    {  
		
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD = data.map 
        { 
            line =>
            line.split("""\""" + delimiter).map(_.toDouble)
        }
        return dataRDD
    }
    
    def FileToRDDIndexedObs(absPath:String, labelDelimiter:String, delimiter:String, sc : SparkContext): RDD[(Double, Array[Double])] =
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
    
    def FileToRDDValidation(absPath:String, sc : SparkContext): RDD[Double] =
    {  
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath).map(_.toDouble)    
        return data
    }
    
    def FileToRDDIndexedValidation(absPath:String, labelDelimiter : String, sc : SparkContext): RDD[(Double, Double)] =
    {  
        println("Now reading... " + absPath)
        val data = sc.textFile(absPath)
        val dataRDD = data.map 
        { 
            line =>
            val parts = line.split("""\""" + labelDelimiter)
            (parts(0).toDouble, parts(1).toDouble)
        }
        return dataRDD
    }

	def getVariableSampleFromObs[T: ClassTag](observations : RDD[Array[T]]): RDD[(Double, Array[T])] =
	{
		observations.zipWithIndex.map{case (k,v) => (v,k)}
			.flatMapValues{ arr => arr.zipWithIndex.map{case (k,v) => (v,k)}}
				.groupBy(element => element._2._1).map{case (l, iter) => (l, iter.toArray.sortBy(e => e._1).map{case (i1, (i2, v)) => v})}
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
		val outputFile = new File(fileName)
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		for (line <- 0 to nbSample - 1)
		{
			val buf = new StringBuilder
			line.toString addString (buf, "", " ", ",")
			for (element <- 0 to sampleLength - 1)
			{				 
				if (element == sampleLength - 1)
					((random * (to - from)) + from).round.toString addString (buf)
				else 
					((random * (to - from)) + from).round.toString  addString (buf, "", " ", " ")
			}
			writer.write(buf.toString)
			if (line != nbSample - 1)
				writer.write("\n")
		}
		writer.close()
	}
	
	def concatenateLines(path : String, repeat : Int, sc : SparkContext) =
	{
		val filesArray = new java.io.File(path).listFiles.filter(_.getName.endsWith(".dat"))
		for(file <- filesArray)
		{
			println("Now writing... ")
			val dir = new File("data_"+(10000*repeat).toString)
			dir.mkdir()
			val splittedFileName = file.toString.split("_")
			val fileName = "Model" + splittedFileName(10) + "_samples" +(10000*repeat).toString + "_" + splittedFileName(12)
			val outputFile = new File(dir, fileName)
			val writer = new PrintWriter(new FileWriter(outputFile))
			val buf = new StringBuilder
			for(i <- 0 to (repeat - 1))
			{
				val data = sc.textFile(file.toString)
				val dataRDD = data.collect.map 
				{ 
					line =>
					line addString (buf, "", "", "\n")
				}	
				writer.write(buf.toString)
				buf.setLength(0)
			}
			writer.close()
		}
	}
	
	def concatenateColumns(path : String, repeat : Int, sc : SparkContext) =
	{
		val filesArray = new java.io.File(path).listFiles.filter(_.getName.endsWith(".dat"))
		for(file <- filesArray)
		{
			println("Now writing... ")
			val dir = new File("variables_"+(200*repeat).toString)
			dir.mkdir()
			val splittedFileName = file.toString.split("_")
			val fileName = "Model_" + splittedFileName(10) + "_variables" +(200*repeat).toString + "_" + splittedFileName(12)
			val outputFile = new File(dir, fileName)
			val writer = new PrintWriter(new FileWriter(outputFile))
			val buf = new StringBuilder
			val data = sc.textFile(file.toString)
			val dataRDD = data.collect.map 
			{ 
					line =>
					for(i <- 0 to (repeat - 1))
					{
						if(i == (repeat -1))
							line addString (buf, "", "", "")
						else
							line addString (buf, "", "", "; ")
					}
					writer.write(buf.toString)
					writer.write("\n")
					buf.setLength(0)
			}
			writer.close()				
		}
	}

	def writeLogValidation(fileName : String, validation: RDD[(Double, Double)]) = {
		println("Now writing... ")
		val outputFile = new File(fileName)
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		validation.collect.foreach{ prob =>
			writer.write(math.log(prob._2).toString + "\n")
		}
		writer.close()
	}
	
	def indexedFile(filename: String, readFilename: String, delimiter: String) =
	{
		val outputFile = new File(filename)
		val writer = new PrintWriter(new FileWriter(outputFile))
		val buf = new StringBuilder
		var i = 0
		for (line <- Source.fromFile(readFilename).getLines()) 
		{
			line addString (buf, i.toString + delimiter + ",", "", "")
			writer.write(buf.toString)
			writer.write("\n")
			buf.setLength(0)
			i = i + 1
		}
		writer.close()
	}
	
	def intList(l : List[String]) = l.map(x=>Integer.parseInt(x))
}
