import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document
	
object StreamingMapper
{
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}

	def getTagValue(document: Document, tag: String) : String =
	{
		document.getElementsByTagName(tag).item(0).getTextContent
	}
	
	def main(args: Array[String])
	{
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)
		
		val sparkConf = new SparkConf().setAppName("WordCount")
		
		// Read the parameters from the config file //////////////////////////
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
		
		println(s"refPath = $refPath\nbwaPath = $bwaPath\nnumTasks = $numTasks\nnumThreads = $numThreads\nintervalSecs = $intervalSecs")
		println(s"streamDir = $streamDir\ninputDir = $inputDir\noutputDir = $outputDir")
		
		// Create stream and output directories if they don't already exist
		new File(streamDir).mkdirs
		new File(outputDir).mkdirs
		//////////////////////////////////////////////////////////////////////
		
		sparkConf.setMaster("local[" + numTasks + "]")
		sparkConf.set("spark.cores.max", numTasks)
		val ssc = new StreamingContext(sparkConf, Seconds(intervalSecs))
		
		// Add your code here. 
		// Use the function textFileStream of StreamingContext to read data as the files are added to the streamDir directory.
		
		ssc.start()
		ssc.awaitTermination()
	}
}
