// sbt assembly help: https://sparkour.urizone.net/recipes/building-sbt/
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io._
import java.util.Locale
import org.apache.commons.lang3.StringUtils

object WordFreqCounts {

  val conf = new SparkConf().setAppName("WordFreqCounts")
  val sc = new SparkContext(conf)

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
    val wordRegex = "([a-zA-Z][\\w']*-?[a-zA-Z]|[a-zA-Z])|([^\\sa-zA-Z])+".r()

    val rdd_text = sc.textFile(inputFile)
    val words = rdd_text
      .map(_.toLowerCase)
      .map(_.replace("\n","."))
      .flatMap(x => wordRegex.findAllIn(x))

    writeToFile(words.collect(), "freq.txt")
  }

  ////

  def writeToFile(content: Array[String], filePath: String): Unit = {
    new File("output").mkdirs
    val pw = new java.io.PrintWriter(new File(filePath))

    try {
      pw.write("Word\n")
      content.foreach(line => pw.write(line + "\n"))
    }
    finally {
      pw.close
    }
  }
}
