/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection.mutable.ArrayBuffer

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
		calculateDensity(dbsnpFile, dictFile)
		println("\n\n------------------------------")

		sc.stop()

		val et = (System.currentTimeMillis - t0) / 1000
		println("{Time taken = %d mins %d secs}".format(et / 60, et % 60))

	}


	def calculateDensity(dbsnpFile: String, dictFile: String): Unit = {

		// (text)
		val dbnsp = sc
			.textFile(dbsnpFile)
			.filter(x => !x.startsWith("#"))                                      // remove header

		// (name, position)
		val positionData = dbnsp
			.map(x => textToPositionData(x))

		// (text)
		val dict = sc
			.textFile(dictFile)
			.mapPartitionsWithIndex{
				(index, row) => if (index == 0) row.drop(1) else row                // remove header
			}

		// (name, total region)
		val totalRegionData = dict
			.map(x => textToDictData(x))
			.filter(x => !x._1.contains("_"))                                     // remove unnecessary chromosome
			.map(x => (x._1, lengthToTotalRegion(x._2)))                          // convert length to total region

		// (name, index)
		val indexData = totalRegionData
			.zipWithIndex()                                                       // get the index
			.map(x => (x._1._1, x._2))


		// (name, list of region)
		val regionListData = totalRegionData
			.map(x => (x._1, regionToList(x._2)))

		// (name, region)
		val regionData = regionListData
			.flatMap(x => regionListToRegion(x))

	}

	def textToPositionData(text: String): (String, Int) = {

		val delimitedText = text.split("\t")
		val name = delimitedText(0)
		val position = delimitedText(1).toInt

		(name, position)
	}

	def textToDictData(text: String): (String, Double) = {

		val delimitedText = text.split("\t|\\:")
		val name = delimitedText(2)
		val length = delimitedText(4).toDouble

		(name, length)

	}

	def lengthToTotalRegion(length: Double): Int = {
		math.ceil(length / 100).toInt
	}

	def regionToList(region: Int): (Seq[Int]) = {

		val regionList = Seq.range(1, region + 1 )

		regionList

	}

	def regionListToRegion (x: (String, Seq[Int])) : Seq[(String, Int)] = {

		val name = x._1
		val regionList = x._2
		val regionData = ArrayBuffer[(String, Int)]()

		for (region <- regionList)
			regionData.append((name, region))

		regionData
	}

}
