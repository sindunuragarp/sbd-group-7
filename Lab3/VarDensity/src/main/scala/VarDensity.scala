/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object VarDensity {

	final val compressRDDs = true

	private val conf = new SparkConf().setAppName("Variant Density Calculator App")
	private val sc = new SparkContext(conf)

		
	def main(args: Array[String]) {
		val tasks = args(0)
		val dbsnpFile = args(1)
		val dictFile = args(2)
		
		println(s"Tasks = $tasks\ndbsnpFile = $dbsnpFile\ndictFile = $dictFile\n")
			

		conf.setMaster("local[" + tasks + "]")
		conf.set("spark.cores.max", tasks)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")
		
		val t0 = System.currentTimeMillis
		
		// Add your main code here

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

		println("------------------------------\n\n")
		calculateDensity(dictFile)
		println("\n\n------------------------------")
		
		sc.stop()
		
		val et = (System.currentTimeMillis - t0) / 1000
		println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))

	}


	def calculateDensity(inputFile: String): Unit = {

		val dict = sc.textFile(inputFile)

		val data = dict
			.map(x => rowToData(x))
			.filter(x => !x._1.contains("_") && !x._1.contains("1.0"))

		val indexedData = data
			.zipWithIndex()
			.map(x => (x._1._1, x._2, x._1._2))

		indexedData.foreach(println)

	}

	def rowToData(text: String): (String, String) = {

		val delimitedText = text.split("\t|\\:")
		val chromosome = delimitedText(2)
		val length = delimitedText(4)

		return (chromosome, length)

	}

}
