package converters

import breeze.linalg.DenseVector
import dataformats.FileData

import scala.io.Source

/**
  * Generic file parser that takes a delimited input file.
  *
  * @param filePath File path as a String
  * @param delimiter Character to delimit file entries
  * @param columnsToDelete Range of columns to remove
  * @param columnsAreVariants Transpose the table if this is true
  */
class CustomFileParser(filePath: String,
                       delimiter: String = "\\t",
                       columnsToDelete: Seq[Int] = Seq(),
                       columnsAreVariants: Boolean = false
                      )
  extends FileParser {

  val fileData: FileData = {
    val splitLines = getProcessedAndSplitFileLines
    val headerLine = splitLines.head
    val dataLines = splitLines.drop(1)

    val dataTuples: Vector[(String, DenseVector[Double])] =
      dataLines.map(
        line => Tuple2(line.head,
                       // Convert the values to doubles, and create a DenseVector with them
                       DenseVector( line.drop(1).map(_.toDouble):_* )
        )
      )
    new FileData(sampleNames = headerLine.drop(1), dataPairs = dataTuples)

  }

  private def getProcessedAndSplitFileLines: Vector[Vector[String]] = {
    val buffSource = Source.fromFile(filePath, FILE_ENCODING)

    val splitLines: Vector[Vector[String]] = buffSource.getLines.toVector.map(_.split(delimiter).toVector)
    val table: Table = new Table(splitLines)

    val filtered = table.deleteColumns(columnsToDelete: _*)
    val vectorWithAnyType = if (columnsAreVariants) filtered.transpose.table else filtered.table
    // Turn this 2D vector of type Any to the type String
    vectorWithAnyType.map(row => row.map(col => col.toString))
  }

}