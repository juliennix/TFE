/////////////////////////////////////////////////////////////////
// Author : Julien Nix thanks to the code of Daniel Gonau      //
// https://dgronau.wordpress.com/2010/11/28/nochmal-kruskal    //        
// For the University of Liège                                 //     
// Kruslal algorithm (MST)                                     //
///////////////////////////////////////////////////////////////// 


package graphicalLearning

object Kruskal 
{
	case class Edge[A](v1:A, v2:A, weight:Double)
 
	type LE[A] = List[Edge[A]]
	type OS[A] = Option[Set[A]]
 
	class SetMap[A](data:Map[A,collection.mutable.Set[A]] = Map[A,collection.mutable.Set[A]]()) 
	{
		import collection.mutable.Set
 
		//checks if a value exists
		def contains(a:A) = data.contains(a);
	 
		//checks if two values are in the same Set
		def sameSet(repr1:A, repr2:A) = data.get(repr1) == data.get(repr2)
	 
		//adds a new Set with one initial value
		def add(a:A) = if (contains(a)) this else new SetMap(data + (a -> Set(a)))
	 
		//adds a new value to an existing Set (specified by repr)
		def insert(a:A, repr:A) = if (contains(a) || ! contains(repr)) this else 
		{
		  val set = data(repr)
		  set += a
		  new SetMap(data + (a -> set))
		}
	 
		//merges two sets (specified by two representants)
		def merge(repr1:A, repr2:A) = if(! contains(repr1) || ! contains(repr2)) this else 
		{
			val set1 = data(repr1)
			val set2 = data(repr2)
			if(set1 eq set2) this else 
			{
				set1 ++= set2
				new SetMap(data ++ set2.map(a => a -> set1))
			}
		}
	}
 
	def kruskal[A](list: LE[A]) = list.sortBy(_.weight).foldLeft((Nil:LE[A],new SetMap[A]()))(mst)._1
 
	def mst[A](t:(LE[A], SetMap[A]), e:Edge[A]) = 
	{
		val ((es, sets), Edge(p,q,_)) = (t,e)
		(sets.contains(p), sets.contains(q)) match 
		{
			case (false, false) => (e :: es, sets.add(p).insert(q,p))
			case (true, false) => (e :: es, sets.insert(q,p))
			case (false, true) => (e :: es, sets.insert(p,q))
			case (true,true) => if (sets.sameSet(p,q)) (es, sets) //Cycle
                            else (e :: es, sets.merge(p,q))
		}
	}


	def kruskalTree(weightMatrix : Array[Array[Float]]): List[Kruskal.Edge[Symbol]] = {
			var nbNodes = weightMatrix.size
			var listEdge = List[Kruskal.Edge[Symbol]]()
			for (i <- 0 to nbNodes-2)
			{
				for (j <- i+1 to nbNodes-1) 
				{
					listEdge = listEdge ::: List(Edge(Symbol(i.toString),Symbol(j.toString),weightMatrix(i)(j)))
				}
			}
			var mst = kruskal(listEdge)
			println(mst)
			return mst
	}
	def kruskalTree(nodeWeightList : List[List[String]]): List[Kruskal.Edge[Symbol]] = {
			var nbNodes = nodeWeightList.size
			var listEdge = List[Kruskal.Edge[Symbol]]()
			for (i <- 0 to nbNodes-1)
			{
				listEdge = listEdge ::: List(Edge(Symbol(nodeWeightList(i)(0)),Symbol(nodeWeightList(i)(1)),nodeWeightList(i)(2).toDouble))
			}
			var mst = kruskal(listEdge)
			println(mst)
			return mst
	}
 
}










