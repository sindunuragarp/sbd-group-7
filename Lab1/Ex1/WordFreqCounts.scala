// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.mllib.rdd.RDDFunctions._

object WordFreqCounts {

  private val conf: SparkConf = new SparkConf().setAppName("WordFreqCounts")
  private val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    // Get input file's name from the command line argument
		val inputFile = args(0)
		println("Input file: " + inputFile)

		// Uncomment these two lines if you want to see a less verbose messages from Spark
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)

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

    // Read input file as rdd of each line
    // >> lines = (line)
    val lines = sc.textFile(inputFile)

    // Split each line into individual words according to the word regex
    // >> words = [(word)]
    val words = lines
      .map(_.toLowerCase) // convert to lower case to avoid case sensitivity
      .map("." + _)       // added full stop at start of each line (no prev word after a newline)
      .flatMap(x => wordRegex.findAllIn(x))

    // Transform as pairs of current and previous word, then convert into tuple format
    // >> wordPairs = [((word, preceded word), 1)]
    val wordPairs = words.sliding(2)
      .map(x => ( (x(1),x(0)), 1 ))

    // Reduce by pair tuple to get frequency of pairs
    // >> pairsFreq = [((word, preceded word), frequency)]
    val pairsFreq = wordPairs
      .filter(x => isWord(x._1._1))
      .reduceByKey(_ + _)

    // Reduce by first word to get frequency of curr word
    // >> currWordsFreq = [(word, frequency)]
    val currWordsFreq = pairsFreq
      .map(x => (x._1._1,x._2))
      .reduceByKey(_ + _)

    // Filter non words to get frequency of actual prev words, and transform to have curr word as key
    // >> prevWordsFreq = [(word, (previous word, frequency)]
    val prevWordsFreq = pairsFreq
      .filter(x => isWord(x._1._2))
      .map(x => ( x._1._1, (x._1._2, x._2) ))

    // Combine frequency of curr and prev word for printing
    // >> wordsCount = [(word, (frequency of word, [(previous word, frequency of previous word)] )]
    val wordsCount = currWordsFreq.cogroup(prevWordsFreq)

    // Write to output
    writeToFile(wordsCount.collect(), "freq.txt")
  }

  ////

  // because of the regex split, all string starting with a letter can be considered a word
  def isWord(text: String): Boolean = {
    text.charAt(0).isLetter
  }

  // sorts the iterables by freq and alphabet, and prints them to the specified output file (inside the output dir)
  def writeToFile(content: Iterable[(String, (Iterable[Int], Iterable[(String, Int)]))], filePath: String): Unit = {
    val outDir = "output"
    new File(outDir).mkdirs
    val writer = new java.io.PrintWriter(new File(outDir + "/" + filePath))

    try {
      content
        .toList.sortBy(x => (x._2._1.head * -1, x._1))
        .foreach(currWord => {
          writer.write(currWord._1 + ":" + currWord._2._1.head + "\n")
          currWord._2._2
            .toList.sortBy(x => (x._2 * -1, x._1))
            .foreach(prevWord => {
              writer.write("\t" + prevWord._1 + ":" + prevWord._2 + "\n")
            })
        })
    }
    finally {
      writer.close()
    }
  }
}
