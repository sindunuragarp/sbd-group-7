// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object WordFreqCounts 
{
	def main(args: Array[String]) 
	{
		val inputFile = args(0) // Get input file's name from this command line argument
		val conf = new SparkConf().setAppName("WordFreqCounts")
		val sc = new SparkContext(conf)
		
		println("Input file: " + inputFile)
		
		// Uncomment these two lines if you want to see a less verbose messages from Spark
		//Logger.getLogger("org").setLevel(Level.OFF);
		//Logger.getLogger("akka").setLevel(Level.OFF);
		
		val t0 = System.currentTimeMillis
		
		val et = (System.currentTimeMillis - t0) / 1000
		System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}
}
