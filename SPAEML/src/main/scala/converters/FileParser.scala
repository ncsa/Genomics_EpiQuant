package converters

import java.io.{BufferedWriter, File, FileWriter}
import dataformats.FileData

abstract class FileParser {

  final val FILE_ENCODING = "UTF-8"

  /** Concrete subclasses automatically parse the input file(s) and transform it into the FileData structure */
  val fileData: FileData

  /**
    * Parse the input files and write the result to a output file.
    * @param outputFilePath Path to the output file
    */
  def writeEpiqFile(outputFilePath: File): Unit = {
    val bw = new BufferedWriter(new FileWriter(outputFilePath))

    val header = "HeaderLine" + "\t" + fileData.sampleNames.mkString("\t")
    val dataConcat = fileData.dataPairs.map(x => {x._1 + "\t" + x._2.toArray.mkString("\t")})

    bw.write(header + "\n")
    dataConcat.foreach(line => bw.write(line + "\n"))

    bw.flush()
    bw.close()
  }



}