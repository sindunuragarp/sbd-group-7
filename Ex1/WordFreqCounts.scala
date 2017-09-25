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

    // Read file and split text into words
    val rddText = sc.textFile(inputFile)
    val words = rddText
      .map("." + _)
      .map(_.toLowerCase)
      .map(_.replaceAll("""\n\r""","."))
      .flatMap(x => wordRegex.findAllIn(x))

    // Transform as pairs of current and previous word
    val wordsSize = words.count()
    val wordsCurr = words.zipWithIndex().filter(x => x._2 != 0).map(_._1)
    val wordsPrev = words.zipWithIndex().filter(x => x._2 != wordsSize-1).map(_._1)
    val wordPairs = wordsCurr.zip(wordsPrev)

    // Group words to count occurences
    val wordsCount = wordPairs
      .groupBy(_._1)
      .filter(x => isWord(x._1))
      .sortBy(_._2.size * -1)
      .map(group => (
        group._1 + ":" + group._2.size,
        group._2
          .groupBy(_._2)
          .filter(x => isWord(x._1))
          .toList.sortBy(_._2.size * -1)
          .map(x => x._1 + ":" + x._2.size)
      ))

    // Write to output
    writeToFile(wordsCount.collect(), "freq.txt")
  }

  ////

  def isWord(text: String): Boolean = {
    return text.charAt(0).isLetter
  }

  def writeToFile(content: Iterable[Tuple2[String,Iterable[String]]], filePath: String): Unit = {
    new File("output").mkdirs
    val pw = new java.io.PrintWriter(new File(filePath))

    try {
      content.foreach(word => {
        pw.write(word._1 + "\n")
        word._2.foreach(prevWord => {
          pw.write("\t" + prevWord + "\n")
        })
      })
    }
    finally {
      pw.close
    }
  }
}
