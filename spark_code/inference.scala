package graphicalLearning

import graphicalLearning.GHS._

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint


// create a bi partite graph, use a BN and fix the direction of the edges in order to create a 
// markov tree. then fill the partite graph trhoug the markov tree and compute the different
// factor/conditional probability 
object Inference extends Serializable
{ 
	def sendMessage(triplet: EdgeTriplet[Double, Double]):  Iterator[(org.apache.spark.graphx.VertexId, Double)] = 
	{ 
		if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity )
			Iterator((triplet.dstId, triplet.srcAttr + 1))
		else if (triplet.dstAttr != Double.PositiveInfinity && triplet.srcAttr == Double.PositiveInfinity)
			Iterator((triplet.srcId, triplet.dstAttr + 1))
		else
			Iterator.empty
	}
	def orientGraph(graph : Graph[Node, Double]) : Graph[Node, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)	
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		math.min(attr, msg), sendMsg = sendMessage, (a,b) => math.min(a,b))
		val rightEdges = bfs.triplets.map{ t => {
			if (t.srcAttr < t.dstAttr) Edge(t.srcId, t.dstId, t.attr)
			else Edge(t.dstId, t.srcId, t.attr)
			}
		}
		val markovTree = Graph(graph.vertices, rightEdges)
		return markovTree
	}	
	def orientedGraph(graph : Graph[Double, Double]) : Graph[Double, Double] = 
	{
		val root: VertexId = graph.pickRandomVertex()
		val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)
		
		val bfs = initialGraph.pregel(Double.PositiveInfinity)((id, attr, msg) => 
		math.min(attr, msg), sendMsg = sendMessage, (a,b) => math.min(a,b))
		val rightEdges = bfs.triplets.map{ t => {
			if (t.srcAttr < t.dstAttr) Edge(t.srcId, t.dstId, t.attr)
			else Edge(t.dstId, t.srcId, t.attr)
			}
		}
		val markovTree = Graph(bfs.vertices, rightEdges)
		return markovTree
	}	
	
	//~ def send(triplet: EdgeTriplet[Double, Double]):  Iterator[(org.apache.spark.graphx.VertexId, (Double, RDD[(Double, Array[Double]))] = 
	//~ { 
		//~ Iterator((tiplet.dstId, ( triplet.srcId, 
	//~ }
	//~ 
	//~ setUpProbNode(vid: VertexId, attr: Map[Double, Double], message: (Long, RDD[(Double, Array[Double]))) = 
	//~ {
		//~ val father = message._1
		//~ val samples = message._2
		//~ if (father == 0.toDouble)
			//~ samples.filter(x => x._1 == vid).mapValues{a => a.groupBy(x => x).map( a => (a._1, a._2.length.toDouble/240))}.first._2
		//~ else
			//~ 
	def mapFunction(length : Int, variable : Array[Double], condition : Array[Double], i :Int = 0, conjMap : Map[(Double,Double), Int] = Map()) : Map[(Double,Double), Int] = 
	{
		if (i == length) return conjMap
		else
		{
			if (conjMap.contains((variable(i), condition(i)))){
				val newConjMap = conjMap.updated((variable(i), condition(i)), conjMap((variable(i), condition(i))) + 1)
				mapFunction(length, variable, condition, i+1, newConjMap)
			}
			else{
				val newConjMap = conjMap + ((variable(i), condition(i)) -> 1)
				mapFunction(length, variable, condition, i+1, newConjMap)
			}
		}	
	}
    def conditionalProb(variable : Array[Double], condition : Array[Double]): Map[(Double, Double), Double] =
    {
		val length = variable.length
		val pY = condition.groupBy(x=>x).mapValues(_.size.toDouble/length)
		val conjMap = mapFunction(length, variable, condition)
		return conjMap.map
		{
			case (key, value) =>
			{
				val norm = (value.toDouble / length)
				(key, (norm / pY(key._2)))
			}
		}
	}  			

	def margProb(l : Array[Double]): Map[(Double, Double), Double] = 
    {
        val length = l.length
        val freq = l.groupBy(x=>(x, x)).mapValues(_.size.toDouble/length)
        return freq.map{ x => x}
    }  

	
	def setUpMarkovTree(markovTree : Graph[Double, Double], samples :  RDD[(Double, Array[Double])]) : Graph[(Map[(Double, Double), Double], Double), Double] = 
	{
		val cart = samples.cartesian(samples).filter{ case ((key1, val1), (key2, val2)) => key1 != key2}
		val keyValue = cart.map{ case ((key1, val1), (key2, val2)) => ((key1.toLong, key2.toLong), (val1, val2))}
		val relation = markovTree.triplets.map(t => ((t.srcId, t.dstId), 0))
		val joined = relation.join(keyValue)
		val c = joined.map{ case ((key1, key2),(val1,(array1, array2))) => (key2, conditionalProb(array1, array2))}
		//~ val labels = markovTree.vertices.filter{ case (vid, level) => level != 0.0}.map(x => (x._1.toDouble, 0))
		//~ val root = samples.subtractByKey(labels).map{ case (label, sample) => (label.toLong, margProb(sample))}
		val rootLabel = markovTree.vertices.filter{ case (vid, level) => level == 0.0}.keys.first
		val root = samples.filter{ case (label, sample) => label == rootLabel}.map{ case (label, sample) => (label.toLong, margProb(sample))}
		val vertices = c union root join markovTree.vertices
		return Graph(vertices, markovTree.edges)
	}	
	
	//~ def beliefPropagation()
	
	def inference(markovTreeSetUp : Graph[(Map[(Double, Double), Double], Double), Double], evidence : Map[Long, (Double, Double)]) =
	{
		//compute the leaves and root
		val max_depth = markovTreeSetUp.vertices.max()(Ordering.by(v => v._2._2))._2._2
		val state = markovTreeSetUp.collectNeighborIds(EdgeDirection.Out).map{ case (key, arr) => if(arr.isEmpty) (key, "leaf") else (key, "node")}
		//~ val state = (markovTreeSetUp.collectNeighborIds(EdgeDirection.Out).map{ case (key, arr) => if(arr.isEmpty) (key, "leaf") else (key, "node")} join
			//~ markovTreeSetUp.collectNeighborIds(EdgeDirection.In).map{ case (key, arr) => if(arr.isEmpty) (key, "root") else (key, "node")}).map{ case (key, (state1, state2)) => 
				//~ if (state1 == state2) (key, "node") else{ if(state1 == "node") (key, state2) else (key, state1)}}
		val joined = (markovTreeSetUp.vertices.join(state)).map{ case( vid, ((cpt, level), state)) => if(level == 0) ( vid, ((cpt, level), "root")) else ( vid, ((cpt, level), state))}
		val init = joined.map{ case (key, ((cpt, level), state)) =>
		{
			val getEvidence = evidence.getOrElse(key, (-1,-1.0))
			state match 
			{
				case "leaf" => { 
					if (getEvidence != (-1,-1.0)){
						val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)}
						(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))}
					else {
						(key, (level, 0.0, state, cpt, cpt.map{ case ((v, cond), value) => (v, 1.0)}, Map[Double,Double](), Map[Double,Double]())) 
					 }}
				case "root" => { 				
					if (getEvidence != (-1,-1.0)){ 
						val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)} 
						(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))}
					else {
						(key, (level, 0.0, state, cpt, Map[Double,Double](), cpt.map{ case ((v, cond), value) => (v, value)}, Map[Double,Double]())) 
					 }}
				case "node" => { 					
					if (getEvidence != (-1,-1.0)) { 
						val init =  cpt.map{ case((v, cond), value)  => if (getEvidence._1 == v) (v, 1.0) else (v, 0.0)} 
						(key, (level, 0.0, state, cpt, init, init, Map[Double,Double]()))}
					else {
						(key, (level, 0.0, state, cpt, Map[Double,Double](), Map[Double,Double](), Map[Double,Double]())) 
					}}
			}
		}}
		
		val evidenceGraph = Graph(init, markovTreeSetUp.edges).cache()
		val initialMessage = (Map[Double, Double](), max_depth)
		
		val lambdaGraph = evidenceGraph.pregel(initialMessage, activeDirection = EdgeDirection.In)(
		vprog = NodeLambdaUpdate,
		sendMsg = updateLambda,
		mergeMsg = combineLambda).cache()
		val cleanVertices = lambdaGraph.vertices.map{ case (key, (level, algoLevel, state, cpt, lambda, pi, belief)) => (key, (state, cpt, lambda, pi, belief))}
		val messageToChildren = cleanVertices.join(lambdaGraph.collectNeighbors(EdgeDirection.Out))
		.map{ case (vid, (attr, arr)) => if(arr.isEmpty) (vid, (attr, null)) 
			else 
				(vid, (attr, computeChildrenMessage(arr.map{ case (k, t) => (k, t._5)})))}
					
		val readyGraph = Graph(messageToChildren , evidenceGraph.edges)
		
		val initialM = Map[Double, Double]()
		val inferedTree = readyGraph.pregel(initialM, activeDirection = EdgeDirection.Out)(
		vprog = NodeUpdate,
		sendMsg = update,
		mergeMsg = combine)
	}

	def computeChildrenMessage(array : Array[(Long, Map[Double, Double])]) : Map[Long, Map[Double, Double]] =
	{
		if(array.length == 1) return array.map{ case(cid, lambda) => (cid, lambda.map{case (k,v) => (k,1.0)})}.toMap
		else
		return array.map{ case (cid, lambda) => (cid, array.filter(a => a._1 != cid).map{case (id, arr2) => arr2}.reduce((a,b) => a.map{case(key, value) => (key, value * b(key))}))}.toMap
	}
	
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
		if(triplet.dstAttr._7 == Map[Double, Double](-1.0 -> -1.0)){
			if (triplet.dstAttr._3 == "leaf") {
				if (triplet.dstAttr._1 == triplet.dstAttr._2)
					Iterator((triplet.srcId, (triplet.dstAttr._4.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
						((cptKeyChild, cptKeyAncestor), cptVal * triplet.dstAttr._5(cptKeyChild))}.groupBy(_._1._2).map{case (k, tr) => 
							(k,tr.values.reduce(_+_))}, triplet.dstAttr._2 - 1)))
				else{
					Iterator((triplet.dstId, (Map[Double,Double](), triplet.dstAttr._2 - 1)))}
			}
			else Iterator.empty
		}
		else if(triplet.dstAttr._7 == Map[Double, Double](0.0 -> 0.0)){
			if (triplet.dstAttr._1 == triplet.dstAttr._2){
				Iterator((triplet.srcId, (triplet.dstAttr._4.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
					((cptKeyChild, cptKeyAncestor), cptVal * triplet.dstAttr._5(cptKeyChild))}.groupBy(_._1._2).map{case (k, tr) => 
						(k,tr.values.reduce(_+_))}, triplet.dstAttr._2 - 1)))}
			else
				Iterator.empty
		}
		else 
			Iterator.empty
	}
	def combineLambda(lambda1 : (Map[Double, Double], Double), lambda2 : (Map[Double, Double], Double)) : (Map[Double, Double], Double)  = 
	{
		if(lambda1._1.isEmpty && lambda2._1.isEmpty)	lambda1
		else
			(lambda1._1.map{ case(key, value) => (key, value * lambda2._1(key))}, lambda1._2)
	}
	
	def multiply(map1 : Map[Double, Double], map2 : Map[Double, Double]) : Map[Double, Double]  = 
	{
		if (map1.isEmpty || map2.isEmpty)	map1
		else
			map1.map{ case(key, value) => (key, value * map2(key))}
	}
	
	def NodeUpdate(vid: VertexId, attr: ((String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), Map[Long, Map[Double, Double]]), message: Map[Double, Double]) = 
	{
		println(attr._1._1, attr._1._4)
		if (message.isEmpty){
			if(attr._1._1 == "root")
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, multiply(attr._1._3, attr._1._4)), attr._2)
			else
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, Map(-1.0 -> -1.0)), attr._2)
		}
		else{
			if(attr._1._4.isEmpty){
				val pi = attr._1._2.map{ case ((cptKeyChild, cptKeyAncestor), cptVal) => 
							((cptKeyChild, cptKeyAncestor), cptVal * message(cptKeyAncestor))}.groupBy(_._1._1).map{case (k, tr) => 
								(k,tr.values.reduce(_+_))}
				((attr._1._1, attr._1._2, attr._1._3, pi, multiply(attr._1._3, pi)), attr._2)}
			else
				((attr._1._1, attr._1._2, attr._1._3, attr._1._4, multiply(attr._1._3, attr._1._4)), attr._2)
			}		
	}
	def update(triplet: EdgeTriplet[((String, Map[(Double, Double), Double], Map[Double, Double], Map[Double, Double], Map[Double, Double]), Map[Long, Map[Double, Double]]), Double]):  Iterator[(org.apache.spark.graphx.VertexId, Map[Double, Double])] =
	{
		if(triplet.srcAttr._1._5 == Map(-1.0 -> -1.0)){
			if (triplet.srcAttr._1._1 == "root")
				Iterator((triplet.dstId, computeMessage(triplet.dstId, triplet.srcAttr._1._4, triplet.srcAttr._2)))
			else 
				Iterator.empty}
		else
			Iterator((triplet.dstId, computeMessage(triplet.dstId, triplet.srcAttr._1._4, triplet.srcAttr._2)))
	}
	def combine(lambda1 : Map[Double, Double], lambda2 : Map[Double, Double]) : Map[Double, Double]  = 
	{
		if(lambda1.isEmpty && lambda2.isEmpty)	lambda1
		else lambda1
	}
	
	def computeMessage(child : Long, pi : Map[Double, Double], childMap : Map[Long, Map[Double, Double]]) : Map[Double, Double] =
	{
		return multiply(pi, childMap(child))
	}
	
	//~ class Node(node_CPT : Map[(Double, Double), Double], node_evidence : Map[Long, (Int, Double)], node_state : String) extends java.io.Serializable
	//~ {
		//~ val CPT = node_CPT
		//~ val state = node_state
		//~ val lambda = 
		//~ val pi = Map[Int,Double]()
		//~ val belief = Map[Int,Double]()
	//~ }
	//~ def uptdate
	
}
