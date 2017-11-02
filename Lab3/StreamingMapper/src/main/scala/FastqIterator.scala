import java.io.File

import scala.collection.AbstractIterator
import scala.io.Source

/*
 * Custom iterator is copied from scala.io.Source.LineIterator
 * - getc() function modified according to fastq format
 * - accepts file as input and converts it to a file iterator using Source.fromFile()
 */
class FastqIterator(file: File) extends AbstractIterator[String] with Iterator[String] {
  private[this] val sb = new StringBuilder
  lazy val iter: BufferedIterator[Char] = Source.fromFile(file).buffered

  ////

  var line = 1
  var firstc = true
  var valid = true

  def getc(): Boolean = hasNext && {
    val ch = iter.next()

    // check validity (line 1 starts with @, and line 2 starts with +)
    if (line == 1 && firstc && ch != '@') {
      valid = false
    }
    if (line == 3 && firstc && ch != '+') {
      valid = false
    }

    // clear buffer & reset position if not valid
    if (isNewline(ch) && !valid) {
      sb.clear()
      line = 0
    }

    // updates position
    firstc = false
    if (isNewline(ch) && line < 5) {
      line += 1
      firstc = true
    }

    // output when 4 valid lines are read
    if (line > 4 && isNewline(ch)) {
      line = 1
      false
    }
    else {
      sb.append(ch)
      true
    }
  }

  ////

  def isNewline(ch: Char): Boolean = {
    ch == '\r' || ch == '\n'
  }

  def hasNext: Boolean = {
    iter.hasNext
  }

  def next: String = {
    sb.clear()
    while (getc()) { }

    sb.toString
  }
}