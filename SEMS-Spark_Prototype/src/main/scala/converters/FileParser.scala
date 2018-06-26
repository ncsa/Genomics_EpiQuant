package converters

import scala.io.Source
import java.io.File

abstract class FileParser() {
  
  /** Reads in a "delimiter" separated file, and returns a new 2D Vector of its contents */
  protected def readFile(filePath: File, delimiter: String): Table = {
    val buffSource = Source.fromFile(filePath)
    new Table(buffSource.getLines.toVector.map(_.split(delimiter).toVector))
  }

  /**
   * Saves the parsed file as a TSV. The location of the output path is handled by the extending class itself
   */
  def saveParsedFile()
  
  /** Returns the full path to the output file */
  def getOutputPath(): String
}