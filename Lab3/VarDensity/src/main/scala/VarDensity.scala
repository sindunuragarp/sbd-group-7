import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.scheduler._
import org.apache.spark.storage.StorageLevel.{MEMORY_ONLY, MEMORY_ONLY_SER}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer


////


object VarDensity {
	final val compressRDDs = true

	private val conf = new SparkConf().setAppName("Variant Density Calculator App")
	private val sc = new SparkContext(conf)

	final val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("output/sparkLog.txt"), "UTF-8"))

	////

	def main(args: Array[String]) {
		val tasks = args(0)
		val dbsnpFile = args(1)
		val dictFile = args(2)

		println(s"Tasks = $tasks\ndbsnpFile = $dbsnpFile\ndictFile = $dictFile\n")

		conf.setMaster("local[" + tasks + "]")
		conf.set("spark.cores.max", tasks)
		if (compressRDDs)
			conf.set("spark.rdd.compress", "true")

		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

    val t0 = System.currentTimeMillis

		////

		sc.addSparkListener(new SparkListener() {
			override def onApplicationStart(applicationStart: SparkListenerApplicationStart) {
				bw.write(getTimeStamp + " Spark ApplicationStart: " + applicationStart.appName + "\n")
				bw.flush()
			}

			override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
				bw.write(getTimeStamp + " Spark ApplicationEnd: " + applicationEnd.time + "\n")
				bw.flush()
			}

			override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
				val map = stageCompleted.stageInfo.rddInfos
				map.foreach(row => {
					if (row.isCached) {
						bw.write(getTimeStamp + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " +
							row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n")
					}
					else if (row.name.contains("rdd_"))
						bw.write(getTimeStamp + row.name + " processed!\n")
					bw.flush()
				})
			}
		})

		////

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
			.filter(record => !record.startsWith("#"))                            // remove header
		dbnsp.setName("rdd_dbnsp")

		// (chromosome-name, position)
		val positionData = dbnsp
			.map(x => textToPositionData(x))
		positionData.setName("rdd_positionData")

		// ((chromosome-name, position), chromosome-region)
		val regionData = positionData
		  .map { case (name, position) => ((name, position), positionToRegionData(position)) }
		regionData.setName("rdd_regionData")

		// ((chromosome-name, chromosome-region), variant)
		val variantData = regionData
		  .map { case ((name, position), region) => ((name, region), 1)}
			.reduceByKey(_ + _)
		variantData.setName("rdd_variantData")


		////


		// (text)
		val dict = sc
			.textFile(dictFile)
			.mapPartitionsWithIndex{
				(index, row) => if (index == 0) row.drop(1) else row                // remove header
			}
		dict.setName("rdd_dict")

		// (chromosome-name, total-chromosome-region)
		val totalRegionData = dict
		  .map(record => textToDictData(record))
		  .filter { case (name, length) => !name.contains("_")}                 // remove unnecessary chromosome
		  .map { case (name, length) => (name, lengthToTotalRegion(length)) }   // convert length to total region
    totalRegionData.persist(if (compressRDDs) MEMORY_ONLY_SER else MEMORY_ONLY)
    totalRegionData.setName("rdd_totalRegionData")

		// (chromosome-name, chromosome-index)
		val indexData = totalRegionData
			.zipWithIndex()                                                       // get the index
		  .map { case ((name, region), index) => (name, index) }
		indexData.setName("rdd_indexData")

		// (chromosome-name, [chromosome-region])
		val regionListData = totalRegionData
		  .map { case (name, region) => (name, regionToList(region)) }
		regionListData.setName("rdd_regionListData")

		// ((chromosome-name, chromosome-region), 0)
		val fullRegionData = regionListData
			.flatMap(list => regionListToRegion(list))
		  .map { case (name, region) => ((name, region), 0) }                   // set 0 as default number
		fullRegionData.setName("rdd_fullRegionData")


		////


		// (chromosome-name, (chromosome-region, variant))
		val mergedData = fullRegionData
			.leftOuterJoin(variantData)
		  .map { case ((name, region), (zero, variant)) =>
					if (variant.isEmpty)
						(name, (region, zero))
					else
						(name, (region, variant.get))
			}
		mergedData.setName("rdd_mergedData")

		// (chromosome-name, chromosome-index, chromosome-region, variant)
		val finalData = mergedData
			.join(indexData)
		  .map { case (name, ((region, variant), index)) =>
				(name, index, region, variant)
			}
			.collect()

		// Writes to file
		writeToFile(finalData, "vardensity.txt")

	}


  ////


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
		Seq.range(1, region + 1 )
	}

	def regionListToRegion (input: (String, Seq[Int])) : Seq[(String, Int)] = {
		val name = input._1
		val regionList = input._2
		val regionData = ArrayBuffer[(String, Int)]()

		for (region <- regionList) {
      regionData.append((name, region))
    }

		regionData
	}

	def writeToFile(content: Array[(String, Long, Int, Int)], outputFileName: String): Unit = {
		val outputDirectory = "output"
		new File(outputDirectory).mkdirs
		val writer = new PrintWriter(new File(outputDirectory + "/" + outputFileName))

		try {
			content
				.sortBy { case (name, index, region, variant) => (index, region) }
				.foreach { case (name, index, region, variant) =>
					writer.write(name + "|" + index + "|" + region + "|" + variant + "\n")
				}
		}
		finally {
			writer.close()
		}
	}


  ////


	def getTimeStamp: String = {
		"[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime) + "] "
	}
}
