/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perform inference on a markovTree                           //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import graphicalLearning.MarkovTreeProposition._

import org.apache.spark.graphx._
import org.apache.spark.SparkContext._

//~ object FirstPhaseNode extends Serializable 
//~ {
	//~ def apply(): FirstPhaseNode = FirstPhaseNode(0D, 0D, "", Map[JointEvent, Probability](), Map[Event, Probability](),  Map[Event, Probability]())
//~ }
//~ case class FirstPhaseNode(level: Double, algoLevel : Double, state : String, cpt: Map[JointEvent, Probability] = Map(), lambda : Map[Event, Probability], pi : Map[Event, Probability], activeNode : Boolean = false) extends Serializable
//~ 
//~ case class FirstPhaseMessage(lambda : Map[Event, Probability], algoLevel : Double) extends Serializable
//~ 
//~ object SecondPhaseNode extends Serializable 
//~ {
	//~ def apply(): SecondPhaseNode = SecondPhaseNode("", Map[JointEvent, Probability](), Map[Event, Probability](), Map[Event, Probability](),  Map[Event, Probability]())
//~ }
//~ case class SecondPhaseNode(state: String, cpt: Map[JointEvent, Probability] = Map(), lamda : Map[Event, Probability], pi : Map[Event, Probability], belief : Map[Event, Probability]) extends Serializable
//~ 
//~ case class SecondPhaseMessage(pi : Map[Event, Probability]) extends Serializable
//~ 
//~ case class Event(value: Double) extends Serializable
//~ {       
	//~ override def equals(other: Any): Boolean = other match 
	//~ { 
      //~ case that: Event => this.value == that.value && this.value == that.value
      //~ case _ => false 
    //~ }
//~ }
//~ 
//~ case class EvidenceSet(evidences : Map[VertexId, Evidence] = Map()) extends Serializable
//~ 
//~ case class Evidence(event : Event = Event(-1D), prob : Probability = Probability(-1D)) extends Serializable
//~ {
	//~ var notFound: Boolean = event.value == -1D
//~ }

object FirstPhaseNode extends Serializable 
{
	def apply(): FirstPhaseNode = FirstPhaseNode(0D, 0D, "", Map[JointEvent, Probability](), Map[Double, Probability](),  Map[Double, Probability]())
}
case class FirstPhaseNode(level: Double, algoLevel : Double, state : String, cpt: Map[JointEvent, Probability] = Map(), lambda : Map[Double, Probability], pi : Map[Double, Probability], activeNode : Boolean = false) extends Serializable

case class FirstPhaseMessage(lambda : Map[Double, Probability], algoLevel : Double) extends Serializable

object SecondPhaseNode extends Serializable 
{
	def apply(): SecondPhaseNode = SecondPhaseNode("", Map[JointEvent, Probability](), Map[Double, Probability](), Map[Double, Probability](), Map[VertexId, Map[Double, Probability]]())
}
case class SecondPhaseNode(state: String, cpt: Map[JointEvent, Probability] = Map(), lambda : Map[Double, Probability], pi : Map[Double, Probability], childrenLambda : Map[VertexId, Map[Double, Probability]], belief : Map[Double, Probability] = Map(-1D -> Probability(-1D))) extends Serializable

case class SecondPhaseMessage(pi : Map[Double, Probability]) extends Serializable

case class EvidenceSet(evidences : Map[VertexId, Evidence] = Map()) extends Serializable

case class Evidence(event : Double = -1D, prob : Probability = Probability(-1D)) extends Serializable
{
	var notFound: Boolean = event == -1D
}


object InferenceProposition extends Serializable
{ 
	def inference(markovTreeSetUp : Graph[MarkovNode, Double], evidenceSet : EvidenceSet) : Graph[SecondPhaseNode, Double] =
	{
		// Compute the leaves and root
		val max_depth = markovTreeSetUp.vertices.max()(Ordering.by{case (vid, node) => node.level})._2.level
		val state = markovTreeSetUp.collectNeighborIds(EdgeDirection.Out).map{ case (key, arr) => if(arr.isEmpty) (key, "leaf") else (key, "node")}
		val joined = (markovTreeSetUp.vertices.join(state)).map{ case( vid, (markovNode, state)) => if(markovNode.level == 0) (vid, (markovNode, "root")) else (vid, (markovNode, state))}
		
		// Compute the evidences, ie the vertices composed of (key, (level of the node -given by the breadth-search-, algo level -to synchronise the lambda messages,
		//, state -leaf,node,root-, condition probability table, lambda))
		val init = joined.map
		{ 
			case (key, (markovNode, state)) =>
			{
				val getEvidence = evidenceSet.evidences.getOrElse(key, Evidence())
				if (!getEvidence.notFound)
				{
						val initVar =  markovNode.cpt.map{ case(jointEvent, prob)  => if (getEvidence.event == jointEvent.variable) (jointEvent.variable, Probability(1D)) else (jointEvent.variable, Probability(0D))}
							(key, FirstPhaseNode(markovNode.level, 0D, state, markovNode.cpt, initVar, initVar))
				}
				else
				{
					state match 
					{
						case "leaf" => 
						{ 
							(key, FirstPhaseNode(markovNode.level, 0D, state, markovNode.cpt, markovNode.cpt.map{ case (jointEvent, value) => (jointEvent.variable, Probability(1D))}, Map[Double, Probability]()))
						}
						case "root" => 
						{ 				
							(key, FirstPhaseNode(markovNode.level, 0D, state, markovNode.cpt, Map[Double, Probability](), markovNode.cpt.map{ case (jointEvent, value) => (jointEvent.variable, value)}))						
						}
						case "node" => 
						{ 					
							(key, FirstPhaseNode(markovNode.level, 0D, state, markovNode.cpt, Map[Double, Probability](), Map[Double, Probability]()))						
						}
					}
				}
			}
		}
		// Compute the graph with the evidence in order to perform inference on it
		val evidenceGraph = Graph(init, markovTreeSetUp.edges).cache()
		
		// Compute the lambda messages in the graph
		val initFirstPhaseMessage = FirstPhaseMessage(Map[Double, Probability](), max_depth)
		val lambdaGraph = evidenceGraph.pregel(initFirstPhaseMessage, activeDirection = EdgeDirection.In)(
		vprog = NodeLambdaUpdate,
		sendMsg = updateLambda,
		mergeMsg = combineLambda).cache()
		
		// Keep only the element in the vertices that are usefull for the pi propagation
		val messageToChildren = lambdaGraph.vertices.join(lambdaGraph.collectNeighbors(EdgeDirection.Out))
		.map{ case (vid, (attr, arr)) => if(arr.isEmpty) (vid, SecondPhaseNode(attr.state, attr.cpt, attr.lambda, attr.pi, Map())) 
			else 
				(vid, SecondPhaseNode(attr.state, attr.cpt, attr.lambda, attr.pi, computeChildrenMessage(arr.map{ case (vid, node) => (vid, node.lambda)})))}
					
		val readyGraph = Graph(messageToChildren , evidenceGraph.edges)
		
		// Compute the pi messages in the graph
		val initSecondPhaseMessage = SecondPhaseMessage(Map[Double, Probability]())
		val inferedTree = readyGraph.pregel(initSecondPhaseMessage, activeDirection = EdgeDirection.Out)(
		vprog = NodeUpdate,
		sendMsg = update,
		mergeMsg = combine)
		
		return inferedTree
	}


	def multiply(map1 : Map[Double, Probability], map2 : Map[Double, Probability]) : Map[Double, Probability]  = 
	{
		if (map1.isEmpty || map2.isEmpty)	map1
		else
			map1.map{ case(key, prob) => (key, Probability(prob.value * map2(key).value))}
	}
	
	def computeChildrenMessage(array : Array[(VertexId, Map[Double, Probability])]) : Map[VertexId, Map[Double, Probability]] =
	{
		if(array.length == 1) return array.map{ case(cid, lambda) => (cid, lambda.map{case (k,v) => (k, Probability(1D))})}.toMap
		else
		return array.map{ case (cid, lambda) => (cid, array.filter{ case (id, l) => l != cid}.map{ case (id, arr) => arr}.reduce((a,b) => a.map{case(key, prob) => (key, Probability(prob.value * b(key).value))}))}.toMap
	}
	
	// First phase : Lambda messages Pregel's function
	
	def NodeLambdaUpdate(vid: VertexId, attr: FirstPhaseNode, message: FirstPhaseMessage) = 
	{
		if (message.lambda.isEmpty) 
		{
			if(attr.state == "leaf") FirstPhaseNode(attr.level, message.algoLevel, attr.state, attr.cpt, attr.lambda, attr.pi, true)
			else FirstPhaseNode(attr.level, message.algoLevel, attr.state, attr.cpt, attr.lambda, attr.pi)
		}
		else
		{ 
			if (attr.lambda.isEmpty) FirstPhaseNode(attr.level, message.algoLevel, attr.state, attr.cpt, message.lambda, attr.pi, true)
			else FirstPhaseNode(attr.level, message.algoLevel, attr.state, attr.cpt, attr.lambda, attr.pi, true)
		}
	}

	def updateLambda(triplet: EdgeTriplet[FirstPhaseNode, Double]):  Iterator[(VertexId, FirstPhaseMessage)] =
	{
		if(triplet.dstAttr.activeNode)
		{
			println(triplet.dstAttr)
			if (triplet.dstAttr.state == "leaf") 
			{
				if (triplet.dstAttr.level == triplet.dstAttr.algoLevel)
					Iterator((triplet.srcId, FirstPhaseMessage(triplet.dstAttr.cpt.map{ case (jointEvent, prob) => 
						((jointEvent.variable, jointEvent.condition), prob.value * triplet.dstAttr.lambda(jointEvent.variable).value)}.groupBy(_._1._2).map{case (k, tr) => 
							(k, Probability(tr.values.reduce(_+_)))}, triplet.dstAttr.algoLevel - 1)))
				else
					Iterator((triplet.dstId, FirstPhaseMessage(Map[Double,Probability](), triplet.dstAttr.algoLevel - 1)))
			}
			else
			{
				if (triplet.dstAttr.level == triplet.dstAttr.algoLevel)
					Iterator((triplet.srcId, FirstPhaseMessage(triplet.dstAttr.cpt.map{ case (jointEvent, prob) => 
							((jointEvent.variable, jointEvent.condition), prob.value * triplet.dstAttr.lambda(jointEvent.variable).value)}.groupBy(_._1._2).map{case (k, tr) => 
								(k,Probability(tr.values.reduce(_+_)))}, triplet.dstAttr.algoLevel - 1)))
				else
					Iterator.empty
			}
		}
		else
			Iterator.empty
	}

	def combineLambda(lambda1 : FirstPhaseMessage, lambda2 : FirstPhaseMessage ): FirstPhaseMessage = 
	{
		if(lambda1.lambda.isEmpty && lambda2.lambda.isEmpty)	
			lambda1
		else
			FirstPhaseMessage(multiply(lambda1.lambda, lambda2.lambda), lambda1.algoLevel)
	}
	
	// Second phase : Pi messages Pregel's function and Belief computation
	
	def computeMessage(child : VertexId, pi : Map[Double, Probability], childMap : Map[VertexId, Map[Double, Probability]]) : Map[Double, Probability] =
	{
		return multiply(pi, childMap(child))
	}
	
	def NodeUpdate(vid: VertexId, attr: SecondPhaseNode, message: SecondPhaseMessage) = 
	{
		if (message.pi.isEmpty)
		{
			if(attr.state == "root")
				SecondPhaseNode(attr.state, attr.cpt, attr.lambda, attr.pi, attr.childrenLambda, multiply(attr.lambda, attr.pi))
			else
				SecondPhaseNode(attr.state, attr.cpt, attr.lambda, attr.pi, attr.childrenLambda)
		}
		else
		{
			if(attr.pi.isEmpty)
			{
				val pi = attr.cpt.map{ case (jointEvent, cptVal) => 
							((jointEvent.variable, jointEvent.condition), cptVal.value * message.pi(jointEvent.condition).value)}.groupBy(_._1._1).map{case (k, tr) => 
								(k, Probability(tr.values.reduce(_+_)))}
				SecondPhaseNode(attr.state, attr.cpt, attr.lambda, pi, attr.childrenLambda, multiply(attr.lambda, pi))
			}
			else
				SecondPhaseNode(attr.state, attr.cpt, attr.lambda, attr.pi, attr.childrenLambda, multiply(attr.lambda, attr.pi))
		}		
	}
	
	def update(triplet: EdgeTriplet[SecondPhaseNode, Double]):  Iterator[(VertexId, SecondPhaseMessage)] =
	{
		if(triplet.srcAttr.belief == Map(-1D -> Probability(-1D)))
		{
			if (triplet.srcAttr.state == "root")
				Iterator((triplet.dstId, SecondPhaseMessage(computeMessage(triplet.dstId, triplet.srcAttr.pi, triplet.srcAttr.childrenLambda))))
			else 
				Iterator.empty
		}
		else
			Iterator((triplet.dstId, SecondPhaseMessage(computeMessage(triplet.dstId, triplet.srcAttr.pi, triplet.srcAttr.childrenLambda))))
	}
	
	def combine(lambda1 : SecondPhaseMessage, lambda2 : SecondPhaseMessage) : SecondPhaseMessage = 
	{
		lambda1
	}	
}
