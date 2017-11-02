/* VarDensity.scala */
/* Author: Hamid Mushtaq */
import java.io._

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

		// ((name, position), region)
		val regionData = positionData
			.map(x => (x, positionToRegionData(x._2)))

		// ((name, region), variant)
		val variantData = regionData
			.map(x => ((x._1._1, x._2), 1))
			.reduceByKey(_ + _)


		//////


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


		// (name, [list of region])
		val regionListData = totalRegionData
			.map(x => (x._1, regionToList(x._2)))

		// ((name, region), 0)
		val fullRegionData = regionListData
			.flatMap(x => regionListToRegion(x))
			.map(x => (x, 0))                                                     // set 0 as default number


		//////


		// (name, (region, variant))
		val mergedData = fullRegionData
			.leftOuterJoin(variantData)
			.map(x =>
				if (x._2._2.isEmpty)
					(x._1._1, (x._1._2, x._2._1))
				else
					(x._1._1, (x._1._2, x._2._2.get))
			)

		// (name, index, region, variant)
		val finalData = mergedData
			.join(indexData)
			.map(x => (x._1, x._2._2, x._2._1._1, x._2._1._2))
			.collect()

		// write to file
		writeToFile(finalData, "vardensity.txt")

	}

	def textToPositionData(text: String): (String, Int) = {

		val delimitedText = text.split("\t")
		val name = delimitedText(0)
		val position = delimitedText(1).toInt

		(name, position)
	}

	def positionToRegionData(position: Int): Int = {

		math.floor(position / 1000000).toInt + 1

	}

	def textToDictData(text: String): (String, Double) = {

		val delimitedText = text.split("\t|\\:")
		val name = delimitedText(2)
		val length = delimitedText(4).toDouble

		(name, length)

	}

	def lengthToTotalRegion(length: Double): Int = {

		math.ceil(length / 1000000).toInt

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

	def writeToFile(content: Array[(String, Long, Int, Int)], outputFileName: String): Unit = {

		val outputDirectory = "output"
		new File(outputDirectory).mkdirs
		val writer = new PrintWriter(new File(outputDirectory + "/" + outputFileName))

		try {
			content
				.sortBy(x => (x._2, x._3))
				.foreach(x => writer.write(x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4 + "\n"))
		}
		finally {
			writer.close()
		}

	}

}
