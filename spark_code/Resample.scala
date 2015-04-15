/////////////////////////////////////////////////////////////////
// Author : Nix Julien                                         //        
// For the University of Liège                                 //     
// Perform resample techniques                                 //
///////////////////////////////////////////////////////////////// 

package graphicalLearning

import java.nio.ByteBuffer
import java.util.{Random => JavaRandom}
import scala.util.hashing.MurmurHash3

import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


object Resample extends Serializable
{
	
	// Simple bootstrap, maybe a way to do that in more efficient way
	def bootstrapRDD (input: RDD[(Double, Array[Double])], seed: Int): RDD[(Double, Array[Double])] = 
	{
		input.map 
		{ 
			case (key, arr) =>
			// Use random seed = seed + partitionIndex + 1 to make generation reproducible.
			val rng = new XORShiftRandom
			val length = arr.length
			rng.setSeed(seed)
			(key, arr.map( element =>
					arr((rng.nextDouble*(length-1)).round.toInt)))
		}
	}
	
	/**
	* This class implements a XORShift random number generator algorithm
	* Source:
	* Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
	* @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
	* This implementation is approximately 3.5 times faster than
	* {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
	* to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
	* uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
	* for each thread.
	*/
	private class XORShiftRandom(init: Long) extends JavaRandom(init) 
	{
		def this() = this(System.nanoTime)
		private var seed = XORShiftRandom.hashSeed(init)
		// we need to just override next - this will be called by nextInt, nextDouble,
		// nextGaussian, nextLong, etc.
		
		override protected def next(bits: Int): Int = 
		{
			var nextSeed = seed ^ (seed << 21)
			nextSeed ^= (nextSeed >>> 35)
			nextSeed ^= (nextSeed << 4)
			seed = nextSeed
			(nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
		}
		
		override def setSeed(s: Long) 
		{
			seed = XORShiftRandom.hashSeed(s)
		}
	}
	
	private object XORShiftRandom 
	{
	/** Hash seeds to have 0/1 bits throughout. */
		private def hashSeed(seed: Long): Long = 
		{
			val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
			MurmurHash3.bytesHash(bytes)
		}
	}
}
