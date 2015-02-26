/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// File manager                                                //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import java.io._
import scala.io.Source
import math.random
import math.round

object FileManager 
{
	// Functions which takes an adequate file and its delimiter
	// Return a list of list of string corresponding to the list 
	// of the samples contained in the file
    def FileReader(absPath:String, delimiter:String): List[List[String]] =
    {
        var MasterList = List(List(""))
        var F : scala.io.Source = null 
        
        try
        {
            println("Now reading... " + absPath)
            F = Source.fromFile(absPath)
            MasterList = F.getLines().toList map
            {
                // String#split() takes a regex, thus escaping.
                _.split("""\""" + delimiter).toList
            }

            return MasterList
        }   
        catch 
        {  
            case e: java.io.FileNotFoundException => 
            {
                println("The file does not exist!")
                return MasterList    
            }
            case e: java.io.IOException =>
            {
                println("Error reading the file!")
                F.close()
                return MasterList
            }
        } 
    }

	// Function which takes a number of rows and the length of them
	// Write a file "test" containing 0-1 values corresponding to a 
	// file test that could be used to proceed performance test
    def writeExample(nbSample : Int, sampleLength : Int) = 
    {
		println("Now writing... ")
		var str = ""
		val outputFile = new File("test")
		val writer = new PrintWriter(new FileWriter(outputFile))
		
		for (line <- 0 to nbSample)
		{
			str = line.toString + 	" "
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
