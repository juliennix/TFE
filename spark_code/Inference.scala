/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perform inference on a markovTree                           //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

object Inference extends Serializable
{ 
	def inference(markovTreeSetUp : Graph[(Map[(Double, Double), Double], Double), Double], evidence : Map[Long, (Double, Double)]) =
	{
		// Compute the leaves and root
		val max_depth = markovTreeSetUp.vertices.max()(Ordering.by(v => v._2._2))._2._2
		val state = markovTreeSetUp.collectNeighborIds(EdgeDirection.Out).map{ case (key, arr) => if(arr.isEmpty) (key, "leaf") else (key, "node")}
		val joined = (markovTreeSetUp.vertices.join(state)).map{ case( vid, ((cpt, level), state)) => if(level == 0) ( vid, ((cpt, level), "root")) else ( vid, ((cpt, level), state))}
		
		// Compute the evidences, ie the vertices composed of (key, (level of the node -given by the breadth-search-, algo level -to synchronise the lambda messages,
		//, state -leaf,node,root-, condition probability table, lambda, pi, belief))
		val init = joined.map
		{ 
			case (key, ((cpt, level), state)) =>
			{
				val getEvidence = evidence.getOrElse(key, (-1,-1.0))
				state match 
				{
					case "leaf" => 
					{ 
						if (getEvidence != (-1,-1.0))
						{
							val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)}
							(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))
						}
						else 
						{
							(key, (level, 0.0, state, cpt, cpt.map{ case ((v, cond), value) => (v, 1.0)}, Map[Double,Double](), Map[Double,Double]())) 
						 }
					}
					case "root" => 
					{ 				
						if (getEvidence != (-1,-1.0))
						{ 
							val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)} 
							(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))
						}
						else 
						{
							(key, (level, 0.0, state, cpt, Map[Double,Double](), cpt.map{ case ((v, cond), value) => (v, value)}, Map[Double,Double]())) 
						 }
					}
					case "node" => 
					{ 					
						if (getEvidence != (-1,-1.0)) 
						{ 
							val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)} 
							(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))
						}
						else 
						{
							(key, (level, 0.0, state, cpt, Map[Double,Double](), Map[Double,Double](), Map[Double,Double]())) 
						}
					}
				}
			}
		}
		// Compute the graph with the evidence in order to perform inference on it
		val evidenceGraph = Graph(init, markovTreeSetUp.edges).cache()
		
		// Compute the lambda messages in the graph
		val initialMessage = (Map[Double, Double](), max_depth)
		val lambdaGraph = evidenceGraph.pregel(initialMessage, activeDirection = EdgeDirection.In)(
		vprog = NodeLambdaUpdate,
		sendMsg = updateLambda,
		mergeMsg = combineLambda).cache()
		
		// Keep only the element in the vertices that are usefull for the pi propagation
		val cleanVertices = lambdaGraph.vertices.map{ case (key, (level, algoLevel, state, cpt, lambda, pi, belief)) => (key, (state, cpt, lambda, pi, belief))}
		val messageToChildren = cleanVertices.join(lambdaGraph.collectNeighbors(EdgeDirection.Out))
		.map{ case (vid, (attr, arr)) => if(arr.isEmpty) (vid, (attr, null)) 
			else 
				(vid, (attr, computeChildrenMessage(arr.map{ case (k, t) => (k, t._5)})))}
					
		val readyGraph = Graph(messageToChildren , evidenceGraph.edges)
		
		// Compute the pi messages in the graph
		val initialM = Map[Double, Double]()
		val inferedTree = readyGraph.pregel(initialM, activeDirection = EdgeDirection.Out)(
		vprog = NodeUpdate,
		sendMsg = update,
		mergeMsg = combine)
	}


	def multiply(map1 : Map[Double, Double], map2 : Map[Double, Double]) : Map[Double, Double]  = 
	{
		if (map1.isEmpty || map2.isEmpty)	map1
		else
			map1.map{ case(key, value) => (key, value * map2(key))}
	}
	
	def computeChildrenMessage(array : Array[(Long, Map[Double, Double])]) : Map[Long, Map[Double, Double]] =
	{
		if(array.length == 1) return array.map{ case(cid, lambda) => (cid, lambda.map{case (k,v) => (k,1.0)})}.toMap
		else
		return array.map{ case (cid, lambda) => (cid, array.filter(a => a._1 != cid).map{case (id, arr2) => arr2}.reduce((a,b) => a.map{case(key, value) => (key, value * b(key))}))}.toMap
	}
	
	// first phase : Lambda messages Pregel's function
	def NodeLambdaUpdate(vid: VertexId, attr: (Double, Double, String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), message: (Map[Double, Double], Double)) = 
	{
		if (message._1.isEmpty) (attr._1, message._2, attr._3, attr._4, attr._5, attr._6 , Map[Double, Double](-1.0 -> -1.0))
		else
		{ 
			if (attr._5.isEmpty) (attr._1, message._2, attr._3, attr._4, message._1, attr._6, Map[Double, Double](0.0 -> 0.0))
			else (attr._1, message._2, attr._3, attr._4, attr._5, attr._6, Map[Double, Double](0.0 -> 0.0))
		}
	}
	
	def updateLambda(triplet: EdgeTriplet[(Double, Double, String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), Double]):  Iterator[(org.apache.spark.graphx.VertexId, (Map[Double, Double], Double))] =
	{
		if(triplet.dstAttr._7 == Map[Double, Double](-1.0 -> -1.0))
		{
			if (triplet.dstAttr._3 == "leaf") 
			{
				if (triplet.dstAttr._1 == triplet.dstAttr._2)
					Iterator((triplet.srcId, (triplet.dstAttr._4.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
						((cptKeyChild, cptKeyAncestor), cptVal * triplet.dstAttr._5(cptKeyChild))}.groupBy(_._1._2).map{case (k, tr) => 
							(k,tr.values.reduce(_+_))}, triplet.dstAttr._2 - 1)))
				else
					Iterator((triplet.dstId, (Map[Double,Double](), triplet.dstAttr._2 - 1)))
			}
			else Iterator.empty
		}
		else if(triplet.dstAttr._7 == Map[Double, Double](0.0 -> 0.0))
		{
			if (triplet.dstAttr._1 == triplet.dstAttr._2)
				Iterator((triplet.srcId, (triplet.dstAttr._4.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
					((cptKeyChild, cptKeyAncestor), cptVal * triplet.dstAttr._5(cptKeyChild))}.groupBy(_._1._2).map{case (k, tr) => 
						(k,tr.values.reduce(_+_))}, triplet.dstAttr._2 - 1)))
			else
				Iterator.empty
		}
		else 
			Iterator.empty
	}
	
	def combineLambda(lambda1 : (Map[Double, Double], Double), lambda2 : (Map[Double, Double], Double)) : (Map[Double, Double], Double)  = 
	{
		if(lambda1._1.isEmpty && lambda2._1.isEmpty)	
			lambda1
		else
			(multiply(lambda1._1, lambda2._1), lambda1._2)
	}
	
	// Second phase : Pi messages Pregel's function and Belief computation
	
	def computeMessage(child : Long, pi : Map[Double, Double], childMap : Map[Long, Map[Double, Double]]) : Map[Double, Double] =
	{
		return multiply(pi, childMap(child))
	}
	
	def NodeUpdate(vid: VertexId, attr: ((String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), Map[Long, Map[Double, Double]]), message: Map[Double, Double]) = 
	{
		if (message.isEmpty)
		{
			if(attr._1._1 == "root")
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, multiply(attr._1._3, attr._1._4)), attr._2)
			else
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, Map(-1.0 -> -1.0)), attr._2)
		}
		else
		{
			if(attr._1._4.isEmpty)
			{
				val pi = attr._1._2.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
							((cptKeyChild, cptKeyAncestor), cptVal * message(cptKeyAncestor))}.groupBy(_._1._1).map{case (k, tr) => 
								(k,tr.values.reduce(_+_))}
				((attr._1._1, attr._1._2, attr._1._3, pi, multiply(attr._1._3, pi)), attr._2)
			}
			else
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, multiply(attr._1._3, attr._1._4)), attr._2)
		}		
	}
	def update(triplet: EdgeTriplet[((String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), Map[Long, Map[Double, Double]]), Double]):  Iterator[(org.apache.spark.graphx.VertexId, Map[Double, Double])] =
	{
		if(triplet.srcAttr._1._5 == Map(-1.0 -> -1.0))
		{
			if (triplet.srcAttr._1._1 == "root")
				Iterator((triplet.dstId, computeMessage(triplet.dstId, triplet.srcAttr._1._4, triplet.srcAttr._2)))
			else 
				Iterator.empty
		}
		else
			Iterator((triplet.dstId, computeMessage(triplet.dstId, triplet.srcAttr._1._4, triplet.srcAttr._2)))
	}
	
	def combine(lambda1 : Map[Double, Double], lambda2 : Map[Double, Double]) : Map[Double, Double]  = 
	{
		if(lambda1.isEmpty && lambda2.isEmpty)	lambda1
		else lambda1
	}	
}
