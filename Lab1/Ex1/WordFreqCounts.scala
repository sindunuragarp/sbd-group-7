// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.rdd.RDDFunctions._
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
    countWords(inputFile)
		val et = (System.currentTimeMillis - t0) / 1000

    System.err.println("Done!\nTime taken = %d mins %d secs".format(et / 60, et % 60))
	}

  ////

  def countWords(inputFile: String): Unit = {

    // Custom regex for splitting text into words (as defined) and non words
    val wordRegex = """([a-zA-Z][\w']*-?[a-zA-Z]+|[a-zA-Z])|([^a-zA-Z\s])+""".r()

    // Read file and split text into words
    val rddText = sc.textFile(inputFile)
    val words = rddText
      .map(_.toLowerCase)
      .map(_.replaceAll("""\n\r""","."))
      .map("." + _)
      .flatMap(x => wordRegex.findAllIn(x))

    // Transform as pairs of current and previous word
    val wordPairs = words.sliding(2).map(x => (x(0),x(1)))

    // Group words to count occurences
    val wordsCount = wordPairs
      .filter(x => isWord(x._1))
      .groupBy(_._1)
      .sortBy(x => (x._2.size * -1, x._1))
      .map(groupCurr => (
        groupCurr._1 + ":" + groupCurr._2.size,
        groupCurr._2
          .filter(x => isWord(x._2))
          .groupBy(_._2)
          .toList.sortBy(x => (x._2.size * -1, x._1))
          .map(groupPrev =>
            groupPrev._1 + ":" + groupPrev._2.size
          )
      ))

    // Write to output
    writeToFile(wordsCount.collect(), "freq.txt")
  }

  ////

  def isWord(text: String): Boolean = {
    return text.charAt(0).isLetter
  }

  def writeToFile(content: Iterable[Tuple2[String,Iterable[String]]], filePath: String): Unit = {
    val outDir = "output"
    new File(outDir).mkdirs
    val writer = new java.io.PrintWriter(new File(outDir + "/" + filePath))

    try {
      content.foreach(currWord => {
        writer.write(currWord._1 + "\n")
        currWord._2.foreach(prevWord => {
          writer.write("\t" + prevWord + "\n")
        })
      })
    }
    finally {
      writer.close
    }
  }
}
