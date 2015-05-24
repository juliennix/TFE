/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Plot                                              //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.SparkContext

import graphicalLearning.ManageFile._
import graphicalLearning.DistributedGraph._
import graphicalLearning.Prims._
import graphicalLearning.Kruskal._	
import graphicalLearning.Boruvka._
import graphicalLearning.GHS._
import graphicalLearning.MarkovTree._
import graphicalLearning.Inference._
import graphicalLearning.MixtureTree._
import graphicalLearning.TestMethod._
import graphicalLearning.Resample._

import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.chart.ChartFactory.createXYLineChart
import org.jfree.chart.ChartFrame
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYDataset
import org.jfree.chart.plot.PlotOrientation
import java.awt.BorderLayout

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

object Plot
{
	def isEmpty[T](rdd : RDD[T]) = 
	{
		rdd.mapPartitions(it => Iterator(!it.hasNext)).reduce(_&&_) 
	}
	
	def createDataset(fileNames : Array[String], repeat : Int, method : Int, sc : SparkContext) : XYSeries =
	{    
		val numberOfFiles = fileNames.length
		val timeArray = new Array[Double](numberOfFiles)
		val numberOfNodes = new Array[Double](numberOfFiles)
		var methodName = ""
		var fileIndex = 0
		for(filename <- fileNames)
		{
			val delimiter = " "
			val labeldelimiter = ","
			val content = FileToPairRDDVar(filename, labeldelimiter, delimiter, sc)
			val meanTime = new Array[Double](repeat)
			numberOfNodes(fileIndex) = getLineNumber(filename).toDouble
			
			for(i <- 0 to repeat - 1)
			{
				
				if (isEmpty(content)) println("Something went wrong, try again")
				else 
				{
				    try
				    {
				        val t1 = System.currentTimeMillis
				        method match
						{
							case 1 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								kruskalEdgesAndVertices(graph4).edges.count
								methodName = "First Kruskal"
							}
							case 2 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								kruskalEdges(graph4).edges.count
								methodName = "Second Kruskal"
							}
							
							case 3 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								PrimsAlgo(graph4).edges.count
								methodName = "First Prim"
							}
							case 4 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								boruvkaAlgo(graph4).edges.count
								methodName = "First Boruvka"
							}
							case 5 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								kruskalEdgeRDD(graph4).edges.count
								methodName = "Third Kruskal"
							}
							case 6 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								PrimsDistFuncEdge(graph4).edges.count
								methodName = "Second Prim"
							}
							case 7 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								PrimsEdge(graph4).edges.count
								methodName = "Third Prim"
							}
							case 8 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								PrimsRDD(graph4).edges.count
								methodName = "Fourth Prim"
							}
							case 9 => 
							{
								val graph4 = RDDFastGraph(content,sc)
								boruvkaDistAlgo(graph4).edges.count
								methodName = "Second Kruskal"
							}
							case 10 => 
							{
								val messageGraph = GHSGraph(content, sc)
								GHSMwst(messageGraph).edges.count
								methodName = "GHS"
							}
						}
				        val t2 = System.currentTimeMillis
				        meanTime(i) = (t2-t1)/1000D
				    } 
				    catch
				    {
				        case e: java.lang.NumberFormatException => 
				        {
							println("Your file is not adequate")
				        }
				        case e: java.lang.UnsupportedOperationException => 
				        {
							println("Your file is not adequate")   
				        }    
				    }
				}
			}
			timeArray(fileIndex) = meanTime.reduce(_+_)/meanTime.length
			fileIndex += 1
		}

		val serie = new XYSeries("method : " + methodName)
		for(i <- 0 to numberOfFiles - 1)
		{	
			serie.add(numberOfNodes(i), timeArray(i))
		}
		return serie
	}
	
	def computationTimeChart(fileNames : Array[String], repeat : Int, methods : Array[Int], sc : SparkContext) = 
	{
		val chartTitle = ""
		val xAxisLabel = "number of nodes"
		val yAxisLabel = "computation time (in second)"
	 
		val dataset = new XYSeriesCollection()
		for(methodId <- methods)
		{
			dataset.addSeries(createDataset(fileNames, repeat, methodId, sc))
		}

		val frame = new ChartFrame(
		"Plot Windows",
		ChartFactory.createXYLineChart(chartTitle,
		        xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, true, false, false)
	 	)
	 	frame.pack()
		frame.setVisible(true)
	}
	
	def createDatasetMixture(fileNames : Array[String], repeat : Int, method : Int, sc : SparkContext) : XYSeries =
	{    
		val numberOfFiles = fileNames.length
		val timeArray = new Array[Double](numberOfFiles)
		val numberOfNodes = new Array[Double](numberOfFiles)
		var methodName = ""
		var fileIndex = 0
		for(filename <- fileNames)
		{
			val delimiter = " "
			val labeldelimiter = ","
			val content = FileToPairRDDVar(filename, labeldelimiter, delimiter, sc)
			val meanTime = new Array[Double](repeat)
			numberOfNodes(fileIndex) = getLineNumber(filename).toDouble
			
			for(i <- 0 to repeat - 1)
			{
				
				if (isEmpty(content)) println("Something went wrong, try again")
				else 
				{
				    try
				    {
				        val t1 = System.currentTimeMillis
				        val numberOfTree = 10
						createMixtureWithBootstrap(sc, content, numberOfTree)
				        val t2 = System.currentTimeMillis
				        meanTime(i) = (t2-t1)/1000D
				    } 
				    catch
				    {
				        case e: java.lang.NumberFormatException => 
				        {
							println("Your file is not adequate")
				        }
				        case e: java.lang.UnsupportedOperationException => 
				        {
							println("Your file is not adequate")   
				        }    
				    }
				}
			}
			timeArray(fileIndex) = meanTime.reduce(_+_)/meanTime.length
			fileIndex += 1
		}

		val serie = new XYSeries("")
		for(i <- 0 to numberOfFiles - 1)
		{	
			serie.add(numberOfNodes(i), timeArray(i))
		}
		return serie
	}
	
	def computationTimeChartMixture(fileNames : Array[String], repeat : Int, methods : Array[Int], sc : SparkContext) = 
	{
		val chartTitle = ""
		val xAxisLabel = "number of nodes"
		val yAxisLabel = "computation time (in second)"
	 
		val dataset = new XYSeriesCollection()
		for(methodId <- methods)
		{
			dataset.addSeries(createDatasetMixture(fileNames, repeat, methodId, sc))
		}

		val frame = new ChartFrame(
		"Plot Windows",
		ChartFactory.createXYLineChart(chartTitle,
		        xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, false, false, false)
	 	)
	 	frame.pack()
		frame.setVisible(true)
	}
	
	def KLDivergenceChart(train : RDD[(Double, Array[Double])], test : RDD[(Double, Array[Double])], validation : RDD[(Double, Double)],  begin : Int, end : Int, step : Int, sc : SparkContext) = 
	{
		val chartTitle = ""
		val xAxisLabel = "number of trees"
		val yAxisLabel = "Kullback-Leibler divergence (in bits)"
		 	
		val serie = new XYSeries("")	
		val dataset = new XYSeriesCollection()
		for(numberOfTree  <- begin to end by step)
		{
			val mixtureTree = createMixtureWithBootstrap(sc, train, numberOfTree)
			val numberOfSample = train.count.toInt
			val score = KLDivergenceRDD(mixtureTree, test, validation, numberOfSample, sc)
			println(numberOfTree, score)
			serie.add(numberOfTree, score)
		}

		dataset.addSeries(serie)

		val frame = new ChartFrame(
		"Plot Windows",
		ChartFactory.createXYLineChart(chartTitle,
		        xAxisLabel, yAxisLabel, dataset, PlotOrientation.VERTICAL, false, false, false)
	 	)
	 	frame.pack()
		frame.setVisible(true)
	}
	
	    
}



