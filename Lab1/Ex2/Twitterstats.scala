import java.io._
import java.text._
import java.util.{Calendar, Locale}
import java.util.regex.Pattern
import javax.xml.parsers.DocumentBuilderFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.twitter._
import org.apache.tika.language.LanguageIdentifier
import twitter4j._

// Check example https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala

object Twitterstats {

  def getTimeStamp: String = {
    new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime)
  }

  def getLangNameFromCode(code: String) : String = {
    new Locale(code).getDisplayLanguage(Locale.ENGLISH)
  }

  def getLang(s: String) : String = {
    val inputStr = s.replaceFirst("RT", "").replaceAll("@\\p{L}+", "").replaceAll("https?://\\S+\\s?", "")
    var langCode = new LanguageIdentifier(inputStr).getLanguage

    // Detect if japanese
    var pat = Pattern.compile("\\p{InHiragana}")
    var m = pat.matcher(inputStr)
    if (langCode == "lt" && m.find)
      langCode = "ja"

    // Detect if korean
    pat = Pattern.compile("\\p{IsHangul}")
    m = pat.matcher(inputStr)
    if (langCode == "lt" && m.find)
      langCode = "ko"

    langCode
  }

  def isEnglish(s: String): Boolean = {
    getLang(s) == "en" || getLang(s) == "no"
  }

  ////

	def main(args: Array[String]): Unit = {
		val file = new File("cred.xml")
		val documentBuilderFactory = DocumentBuilderFactory.newInstance
		val documentBuilder = documentBuilderFactory.newDocumentBuilder
		val document = documentBuilder.parse(file)
			
		// Configure Twitter credentials
		val consumerKey = document.getElementsByTagName("consumerKey").item(0).getTextContent 				
		val consumerSecret = document.getElementsByTagName("consumerSecret").item(0).getTextContent 		
		val accessToken = document.getElementsByTagName("accessToken").item(0).getTextContent 				
		val accessTokenSecret = document.getElementsByTagName("accessTokenSecret").item(0).getTextContent	

		// Set verbosity to low
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		Logger.getRootLogger.setLevel(Level.OFF)

		// Set the system properties so that Twitter4j library used by twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    // Setup app and stream
		val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
		val ssc = new StreamingContext(sparkConf, Seconds(2)) // Read stream every 2 seconds
		val stream = TwitterUtils.createStream(ssc, None)
		
		// Start stream processing
		handleTweetStream(stream)
		ssc.start()
		ssc.awaitTermination()
	}

  ////

  def handleTweetStream(tweets: DStream[Status]): Unit = {

    // only take english tweets from stream
    val englishTweets = tweets.filter(x => isEnglish(x.getText))

    // extract target information from tweets
    val tweetInfos = englishTweets.map(x => extractTweetInfo(x))

    // group tweets for every time window
    val tweetBuckets = tweetInfos.reduceByKeyAndWindow( (a: TweetInfo, b: TweetInfo) =>
      new TweetInfo(
        a.userName,
        a.text,
        a.hashtags,
        Math.max(a.maxCount, b.maxCount),
        Math.min(a.minCount, b.minCount)
      ),
      Seconds(120), Seconds(2)
    )

    // expand tweets based on hashtags
    val tagsTweet = tweetBuckets.flatMap(tweet => tweet._2.hashtags
      .map(tag => (
        tag, (
          tweet._2.userName,
          tweet._2.text,
          tweet._2.maxCount - tweet._2.minCount - 1
        )
      ))
    )

    // group by hashtags, and filter single use tags
    val tagGroups = tagsTweet.groupByKey()
      .filter(tag => tag._2.size == 1 && tag._2.head._3 <= 1)

    // print on each time windows
    tagGroups.foreachRDD(rdd =>
      printTweets(rdd.collect(), "tweets.txt")
    )
  }

  ////

  def printTweets(tagGroups: Array[(String, scala.Iterable[(String, String, Int)])], filePath: String ): Unit = {

    val outDir = "output"
    new File(outDir).mkdirs
    val writer = new java.io.PrintWriter(
      new FileOutputStream(
        new File(outDir + "/" + filePath),
        true // append to file
      )
    )

    //// Sort and format tweets ////

    val sortedTweets = tagGroups
      .filter(tag => !tag._1.equals("None"))
      .sortBy(tag => tag._2.size * -1)
      .map(tag => tag._2.toList
        .sortBy(tweet => tweet._3 * -1)
        .map(tweet => (tag._1, tag._2.size, tweet))
      )

    val emptyHashtagTweets = tagGroups
      .filter(tag => tag._1.equals("None"))
      .map(tag => tag._2.toList
        .sortBy(tweet => tweet._3 * -1)
        .map(tweet => (tag._1, tag._2.size, tweet))
      )

    //// Print to file ////

    try {
      val sortedTweetsSize = sortedTweets.flatten.length

      sortedTweets
        .flatten
        .zipWithIndex
        .foreach(tweet =>
          writer.append(formatOutput(tweet))
        )

      emptyHashtagTweets
        .flatten
        .zipWithIndex
        .foreach(tweet =>
          writer.append(formatOutput((tweet._1, sortedTweetsSize + tweet._2)))
        )
    } finally {
      writer.close()
    }

    //// Print top ten to output ////

    val topTenTags = sortedTweets
      .take(10)
      .flatMap(tag => tag.take(3))

    val topTenTagsCombined = topTenTags ++ emptyHashtagTweets.flatten.take(10)

    topTenTagsCombined.take(10)
      .zipWithIndex.foreach(tweet =>
        print(formatOutput(tweet))
      )
  }

  def formatOutput(content: ((String, Int, (String, String, Int)), Int) ): String = {
    content._2 + ". " +
      content._1._2 + " " +
      content._1._1 + ":" +
      content._1._3._1 + ":" +
      content._1._3._3 + " " +
      content._1._3._2 +
      "\n-----------------------------------\n"
  }

  ////

  class TweetInfo(val userName: String, val text: String, val hashtags: Array[String], val maxCount: Int, val minCount: Int)

  def extractTweetInfo(status: Status): (Long, TweetInfo) = {
    val x =
      if (status.isRetweet) status.getRetweetedStatus
      else status

    val tags = // Handle empty hashtags
      if (x.getHashtagEntities.isEmpty) Array("None")
      else x.getHashtagEntities.map(tag => "#" + tag)

    val tweetCount = x.getRetweetCount + 1

    (x.getId, new TweetInfo(
      x.getUser.getScreenName,
      x.getText,
      tags,
      tweetCount,
      tweetCount
    ))
  }
}

