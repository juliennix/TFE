/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// main scala file                                             //
///////////////////////////////////////////////////////////////// 

import graphicalLearning.Kruskal._
import graphicalLearning.FileManager._
import graphicalLearning.Network._
import graphicalLearning.Bayesian_network._

object Main 
{
    def main(args:Array[String]) = 
    {
		
        print("Please enter your name's textfile : " )
        //~ val filename = Console.readLine
        val filename = "3poss"
      
        print("Please enter your delimiter in this file : " )
        //~ val delimiter = Console.readLine
        val delimiter = " "
        
        var FileContents = FileReader(filename, delimiter)
   
		//~ var mst = kruskalTree(FileContents)
		//~ networkCreation(mst)
  
        var varList = List(List())
        
        if (FileContents(0).isEmpty == true) println("Something went wrong, try again")
        else 
        {
            try
            {
                // networkCreation(kruskalTree(FileContents))
                // Convert the String read from the file to a list of Integer
                var varList = FileContents.map(x => intList(x))
                // Compute the MST thanks to the mutual information
                // matrix and the Kruskal algorithm
                var mst = skelTree(varList)
                networkCreation(mst)
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
}
