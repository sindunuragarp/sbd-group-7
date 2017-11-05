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
  val streamBatchSize = 500
  val streamBatchInterval = 100 //milliseconds


  ////

  /*
   * Basic utility functions
   */
  def getTimeStamp: String = {
    new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime)
  }

  def getTagValue(document: Document, tag: String) : String = {
    document.getElementsByTagName(tag).item(0).getTextContent
  }

  /*
   * Accepts an array of string and compresses it to a gzip file
   */
  def saveGzip(data: Array[String], path: String): Unit = {
    val file = new File(path)
    val outputStream = new FileOutputStream(file)
    val zipOutputStream = new GZIPOutputStream(outputStream)

    data.map(_+"\n").foreach(x => zipOutputStream.write(x.getBytes))
    zipOutputStream.close()
  }

  /*
   * Runs the BWA using a gzip input and saves it as a sam file
   */
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

    // Prepares the streaming context
    sparkConf.setMaster("local[" + numTasks + "]")
    sparkConf.set("spark.cores.max", numTasks)
    val ssc = new StreamingContext(sparkConf, Seconds(intervalSecs))

    // Creates thread for running the driver
    val driver = new Thread {
      override def run(): Unit = runDriver(inputDir, streamDir, tmpDir)
    }

    // Batches text data every interval, and then processes each rdd batch
    ssc.textFileStream("file://" + streamDirFile.getAbsolutePath)
      .foreachRDD(rdd => {

        // batch = Array[lines]
        val batch = rdd.collect()

        // fastq format from driver is already valid, therefore one read = 4 consequent lines
        val size = batch.length / 4
        println(s"Processing $size reads")

        // Creates tmp gzip file for input to BWA
        val uuid = UUID.randomUUID()
        val tmpFile = s"$tmpDir/chunk_$uuid.fq.gz"
        saveGzip(batch, tmpFile)

        // Calls BWA to process tmp file
        val outFile = s"$outputDir/chunk_$uuid.sam"
        bwaRun(tmpFile, outFile, bwaPath, refPath, numThreads)
        new File(tmpFile).delete()
      })

    //////////////////////////////////////////////////////////////////////

    // Starts both streaming context and driver
    ssc.start()
    driver.start()

    // Waits for driver to finish sending all data, and then terminates streaming after timeout
    driver.join()
    ssc.awaitTerminationOrTimeout(intervalSecs * 5)

    // Delete temporary directory
    val tmpDirFile = new File(tmpDir)
    tmpDirFile.listFiles().foreach(file => file.delete())
    tmpDirFile.delete()
  }


  ////


  /*
   * Driver function reads the fastq files using custom iterator and interleaves each read
   * Iterator is lazy therefore able to process large files efficiently
   */
  def runDriver(inputDir: String, streamDir: String, tmpDir: String): Unit = {
    println("Starting driver")

    // Reads the fastq file pairs
    val dir = new File(inputDir)
    if (!dir.exists() || !dir.isDirectory) {
      println("Input directory doesn't exist!")
      return
    }

    // Sorts file by name to properly get first and second fastq file
    // Assumes there are only two fastq file in the directory and it is named ***1.fastq and ***2.fastq
    val files = dir.listFiles().sortBy(x => x.getName)
    println("File 1 = " + files(0).getAbsolutePath)
    println("File 2 = " + files(1).getAbsolutePath)

    // Custom iterator to read valid inputs based on fastq format
    val file1 = new FastqIterator(files(0))
    val file2 = new FastqIterator(files(1))

    // Interleaves the file by zipping each subsequent read, and writes the result in small chunks
    file1.zip(file2)
      .grouped(streamBatchSize)
      .zipWithIndex
      .foreach{case(tuples, index) =>

        // File is written to tmp directory
        val filename = "stream_fastq_" + index
        val file = new File(tmpDir + "/" + filename + ".tmp")
        val bw = new BufferedWriter(new FileWriter(file))

        // Writes the file incrementally on each read
        tuples.foreach{case(a,b) =>
          bw.write(a + "\n")
          bw.write(b + "\n")
        }
        bw.close()

        // Moves completed file to stream directory so that it gets detected by the stream
        file.renameTo(new File(streamDir + "/" + filename + ".txt"))

        // Adds streaming interval to simulate streaming delay
        Thread.sleep(streamBatchInterval)
      }
  }
}
