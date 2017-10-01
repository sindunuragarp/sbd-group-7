import java.io._
import java.text._
import java.time.LocalDate
import java.util.regex.Pattern
import java.util.{Calendar, Locale}
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
		processTweetStream(stream)
		ssc.start()
		ssc.awaitTermination()
	}


  ////


  def processTweetStream(tweets: DStream[Status]): Unit = {

    // only take english tweets from stream
    // >> englishTweets = stream[status]
    val englishTweets = tweets.filter(x => isEnglish(x.getText))

    // extract target information from tweets
    // >> tweetInfos = stream[(tweet_id, tweetInfo)]
    // >> tweetInfo = (username, text, array[hashtags], maxCount, minCount)
    val tweetInfos = englishTweets.map(x => extractTweetInfo(x))

    // group tweets for every time window, and compute max and min count within window
    // >> tweetBuckets = stream[(tweet_id, tweetInfo)]
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

    // expand tweets based on hashtags + calculate actual count
    // >> tagsTweet = stream[(tag, tagInfo)]
    // >> tagInfo = (username, text, tweetCount)
    val tagsTweet = tweetBuckets.flatMap(tweet => tweet._2.hashtags
      .map(tag => (
        tag, new TagInfo(
          tweet._2.userName,
          tweet._2.text,
          tweet._2.maxCount - tweet._2.minCount + 1
        )
      ))
    )

    // group by hashtags, and filter out single use tags
    // >> tagGroups = stream[(tag, [tagInfo])]
    val tagGroups = tagsTweet.groupByKey()
      .filter(tag => !(tag._2.size == 1 && tag._2.head.count <= 1))

    // for each rdd time window, collect data and print to output
    tagGroups.foreachRDD(rdd =>
      printTweets(rdd.collect(), "tweets.txt")
    )
  }


  ////


  def printTweets(tagGroups: Array[(String, scala.Iterable[TagInfo])], filePath: String ): Unit = {

    val date = new java.util.Date()
    val sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    val formattedDate = sdf.format(date)

    //// Open file ////

    val outDir = "output"
    new File(outDir).mkdirs
    val writer = new java.io.PrintWriter(
      new FileOutputStream(
        new File(outDir + "/" + filePath),
        true // append to file
      )
    )

    //// Sort and format tweets to single tuple with all info ////
    // >> data = [(tag, tagCount, tagInfo)]
    // >> * data still grouped per tag

    val sortedTweets = tagGroups
      .filter(tag => !tag._1.equals("None"))
      .sortBy(tag => tag._2.size * -1)
      .map(tag => tag._2.toList
        .sortBy(info => info.count * -1)
        .map(info => (tag._1, tag._2.size, info))
      )

    val emptyHashtagTweets = tagGroups
      .filter(tag => tag._1.equals("None"))
      .map(tag => tag._2.toList
        .sortBy(info => info.count * -1)
        .map(info => (tag._1, 0, info))
      )

    //// Print to file ////
    // >> dataWithIndex = [((tag, tagCount, tagInfo), index)]
    // >> * data flattened to a single list and appended with its index

    try {
      writer.append("===================================================================\n")
      writer.append("Time : " + formattedDate + "\n")
      writer.append("===================================================================\n")

      val sortedTweetsSize = sortedTweets.flatten.length

      sortedTweets
        .flatten
        .zipWithIndex
        .foreach(info =>
          writer.append(formatOutput(info))
        )

      emptyHashtagTweets
        .flatten
        .zipWithIndex
        .foreach(info =>
          writer.append(formatOutput((info._1, sortedTweetsSize + info._2)))
        )
    } finally {
      writer.close()
    }

    //// Print top ten to output ////

    println("===================================================================")
    println("Time : " + formattedDate)
    println("===================================================================")

    // Take first ten tags and then take 3 from each
    // >> data = [(tag, tagCount, tagInfo)]
    val topTenTags = sortedTweets
      .take(10)
      .flatMap(tag => tag.take(3))

    // Add tweets from no tags group
    // >> data = [(tag, tagCount, tagInfo)]
    val topTenTagsCombined = topTenTags ++ emptyHashtagTweets.flatten.take(10)

    // Take top 10 of combined tags
    // >> dataWithIndex = [((tag, tagCount, tagInfo), index)]
    topTenTagsCombined.take(10)
      .zipWithIndex.foreach(tweet =>
        print(formatOutput(tweet))
      )
  }

  // format output according to example
  def formatOutput(content: ((String, Int, TagInfo), Int) ): String = {
    1+content._2 + ". " +
      content._1._2 + " " +
      content._1._1 + ":" +
      content._1._3.userName + ":" +
      content._1._3.count + " " +
      content._1._3.text +
      "\n-----------------------------------\n"
  }


  ////


  // TweetInfo and TagInfo class to make the code easier to read
  class TweetInfo(
    val userName: String,
    val text: String,
    val hashtags: Array[String],
    val maxCount: Int,
    val minCount: Int
  ) extends Serializable

  class TagInfo(
    val userName: String,
    val text: String,
    val count: Int
  ) extends Serializable

  // function to extract needed information from twitter4j Status
  def extractTweetInfo(status: Status): (Long, TweetInfo) = {

    // Take actual tweet if it is a retweet
    val x =
      if (status.isRetweet) status.getRetweetedStatus
      else status

    // Handle empty hashtags by inserting None tag
    // Actual hashtags are given a hash to differentiate
    val tags =
      if (x.getHashtagEntities.isEmpty) Array("None")
      else x.getHashtagEntities.map(tag => "#" + tag.getText)

    // Take count from global retweetCount
    // count is stored as maxCount and minCount to get actual count in window after grouping
    val tweetCount = x.getRetweetCount

    (x.getId, new TweetInfo(
      x.getUser.getScreenName,
      x.getText,
      tags,
      tweetCount,
      tweetCount
    ))
  }
}

