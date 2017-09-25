// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object WordFreqCounts {

  private val conf: SparkConf = new SparkConf().setAppName("WordFreqCounts")
  private val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    // Get input file's name from this command line argument
		val inputFile = args(0)
		println("Input file: " + inputFile)

		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

    // Run and time program
		val t0 = System.currentTimeMillis
    extractWords(inputFile)
		val et = (System.currentTimeMillis - t0) / 1000

    System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}

  ////

  def extractWords(inputFile: String): Unit = {
    val wordRegex = """([a-zA-Z][\w']*-?[a-zA-Z]+|[a-zA-Z])|([^a-zA-Z\s])+""".r()

    val rddText = sc.textFile(inputFile)
    val words = rddText
      .map("." + _)
      .map(_.toLowerCase)
      .map(_.replaceAll("""\n\r""","."))
      .flatMap(x => wordRegex.findAllIn(x))

    val wordsSize = words.count()
    val wordsNext = words.zipWithIndex().filter((word, i) => i != 0).map(_._1)
    val wordsPrev = words.zipWithIndex().filter((word, i) => i != wordsSize-1).map(_._1)
    val wordPairs = wordsNext.zip(wordsPrev)

    writeToFile(wordPairs.collect(), "freq.txt")
  }

  ////

  def writeToFile(content: Array[Tuple2[String,String]], filePath: String): Unit = {
    new File("output").mkdirs
    val pw = new java.io.PrintWriter(new File(filePath))

    try {
      pw.write("Word, Prev\n")
      content.foreach(line => pw.write(line._1 + "," + line._2 + "\n"))
    }
    finally {
      pw.close
    }
  }
}
