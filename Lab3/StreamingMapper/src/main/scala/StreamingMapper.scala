import java.io._
import java.text._
import java.util.zip.GZIPOutputStream
import java.util.{Calendar, UUID}
import javax.xml.parsers.DocumentBuilderFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.w3c.dom.Document

import sys.process._

object StreamingMapper {
  val streamBatchSize = 1000
  val streamBatchInterval = 50 //milliseconds


  ////


  def getTimeStamp: String = {
    new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime)
  }

  def getTagValue(document: Document, tag: String) : String = {
    document.getElementsByTagName(tag).item(0).getTextContent
  }

  def saveGzip(data: Array[String], path: String): Unit = {
    val file = new File(path)
    val outputStream = new FileOutputStream(file)
    val zipOutputStream = new GZIPOutputStream(outputStream)

    data.foreach(x => zipOutputStream.write(x.getBytes))
    zipOutputStream.close()
  }

  def bwaRun(inPath: String, outPath: String, bwaPath: String, refPath: String, numThreads: String): Unit = {
    println(s"Running bwa for $inPath")

    val out = new File(outPath)
    val cmd = Seq(bwaPath, "mem", refPath, "-p", "-t", numThreads, inPath)

    (cmd #> out).!
  }


  ////


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("Executor")

    //////////////////////////////////////////////////////////////////////

    // Read the parameters from the config file
    val file = new File("config.xml")
    val documentBuilderFactory = DocumentBuilderFactory.newInstance
    val documentBuilder = documentBuilderFactory.newDocumentBuilder
    val document = documentBuilder.parse(file)

    val refPath = getTagValue(document, "refPath")
    val bwaPath = getTagValue(document, "bwaPath")
    val numTasks = getTagValue(document, "numTasks")
    val numThreads = getTagValue(document, "numThreads")
    val intervalSecs = getTagValue(document, "intervalSecs").toInt
    val streamDir = getTagValue(document, "streamDir")
    val inputDir = getTagValue(document, "inputDir")
    val outputDir = getTagValue(document, "outputDir")
    val tmpDir = "tmp"

    println(s"refPath = $refPath\nbwaPath = $bwaPath\nnumTasks = $numTasks\nnumThreads = $numThreads\nintervalSecs = $intervalSecs")
    println(s"streamDir = $streamDir\ninputDir = $inputDir\noutputDir = $outputDir")

    // Create stream and output directories if they don't already exist
    new File(streamDir).mkdirs
    new File(outputDir).mkdirs
    new File(tmpDir).mkdirs()

    // Delete contents of stream & output dir
    val streamDirFile = new File(streamDir)
    val outputDirFile = new File(outputDir)
    streamDirFile.listFiles().foreach(file => file.delete())
    outputDirFile.listFiles().foreach(file => file.delete())

    //////////////////////////////////////////////////////////////////////

    sparkConf.setMaster("local[" + numTasks + "]")
    sparkConf.set("spark.cores.max", numTasks)
    val ssc = new StreamingContext(sparkConf, Seconds(intervalSecs))
    val driver = new Thread {
      override def run(): Unit = runDriver(inputDir, streamDir, tmpDir)
    }

    //////////////////////////////////////////////////////////////////////

    ssc.textFileStream("file://" + streamDirFile.getAbsolutePath)
      .foreachRDD(rdd => {
        val batch = rdd.collect()
        val size = batch.length / 4
        println(s"Processing $size reads")

        // Create tmp file
        val uuid = UUID.randomUUID()
        val tmpFile = s"$tmpDir/chunk_$uuid.fq.gz"
        saveGzip(batch, tmpFile)

        // Call BWA
        val outFile = s"$outputDir/chunk_$uuid.sam"
        bwaRun(tmpFile, outFile, bwaPath, refPath, numThreads)
        new File(tmpFile).delete()
      })

    //////////////////////////////////////////////////////////////////////

    ssc.start()
    driver.start()
    driver.join()
    ssc.awaitTermination()

    // Delete temporary directory
    val tmpDirFile = new File(tmpDir)
    tmpDirFile.listFiles().foreach(file => file.delete())
    tmpDirFile.delete()
  }


  ////


  def runDriver(inputDir: String, streamDir: String, tmpDir: String): Unit = {
    println("Starting driver")

    val dir = new File(inputDir)
    if (!dir.exists() || !dir.isDirectory) {
      println("Input directory doesn't exist!")
      return
    }

    val files = dir.listFiles().sortBy(x => x.getName)
    val file1 = new FastqIterator(files(0))
    val file2 = new FastqIterator(files(1))

    file1.zip(file2)
      .grouped(streamBatchSize)
      .zipWithIndex
      .foreach{case(tuples, index) =>
        val filename = "stream_fastq_" + index
        val file = new File(tmpDir + "/" + filename + ".tmp")
        val bw = new BufferedWriter(new FileWriter(file))

        tuples.foreach{case(a,b) =>
          bw.write(a + "\n")
          bw.write(b + "\n")
        }
        bw.close()

        file.renameTo(new File(streamDir + "/" + filename + ".txt"))
        Thread.sleep(streamBatchInterval)
      }
  }
}
