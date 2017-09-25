import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.io._
import java.util.Locale
import java.text._
import java.net._
import java.util.Calendar
import org.apache.tika.language.LanguageIdentifier
import java.util.regex.Matcher
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.parsers.DocumentBuilder
import org.w3c.dom.Document

// Check example https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala

object Twitterstats
{ 
	def getTimeStamp() : String =
	{
		return new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime())
	}
	
	def getLang(s: String) : String =
	{
		val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
		var langCode = new LanguageIdentifier(inputStr).getLanguage
		
		// Detect if japanese
		var pat = Pattern.compile("\\p{InHiragana}") 
		var m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ja"
		// Detect if korean
		pat = Pattern.compile("\\p{IsHangul}");
		m = pat.matcher(inputStr)
		if (langCode == "lt" && m.find)
			langCode = "ko"
		
		return langCode
	}
  
	def getLangNameFromCode(code: String) : String =
	{
		return new Locale(code).getDisplayLanguage(Locale.ENGLISH)
	}
  
	def main(args: Array[String])
	{
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file);
			
		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent 				
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent 		
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent 				
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent	
		
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

		val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

		val ssc = new StreamingContext(sparkConf, Seconds(2))
		val stream = TwitterUtils.createStream(ssc, None)
		
		// Insert your code here
		
		ssc.start()
		ssc.awaitTermination()
	}
}

