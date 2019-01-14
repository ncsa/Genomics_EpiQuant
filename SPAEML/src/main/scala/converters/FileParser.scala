package converters

import java.io.{BufferedWriter, File, FileWriter}
import dataformats.FileData

abstract class FileParser {

  final val FILE_ENCODING = "UTF-8"

  /** Concrete subclasses automatically parse the input file(s) and transform it into the FileData structure */
  val fileData: FileData

  /**
    * Parse the input files and write the result to an output file in ".epiq" format.
    *
    * Usually, one thinks of samples as rows and variants/phenotypes as columns
    *
    *   (Typical genotype table)               (Typical phenotype table)
    *
    *   Sample SNP1 SNP2 SNP3 ... SNPM         Sample  Pheno1  Pheno2 ...
    *   A      A1   A2   A3       AM           A       A_Ph1   A_Ph2
    *   B      B1   B2   B3       BM           B       B_Ph1   B_Ph2
    *   C      C1   C2   C3       CM           C       C_Ph1   C_Ph2
    *   ...                                    ...
    *   N      N1   N2   N3       NM           N       N_Ph1   N_PhN
    *
    * The ".epiq" format has rows are variants and columns are samples
    *   (Also, a string called "Placeholder" is put in the top-left corner to make everything line up easily)
    *
    *   (genotype ".epiq" file)                  (phenotype ".epiq" file)
    *
    *   Placeholder A   B   C  ... N           Placeholder A      B      C     ... N
    *   SNP1        A1  B1  C1     N1          Pheno1      A_Ph1  B_Ph1  C_Ph1     N_Ph1
    *   SNP2        A2  B2  C2     N2          Pheno2      A_Ph2  B_Ph2  C_Ph2     N_Ph2
    *   SNP3        A3  B3  C3     N3          ...
    *   ...
    *   SNPM        AM  BM  CM     NM
    *
    * @param outputFilePath Path to the output file
    *
    */
  def writeEpiqFile(outputFilePath: String): Unit = {
    val file = new File(outputFilePath)
    val bw = new BufferedWriter(new FileWriter(file))

    val header = "HeaderLine" + "\t" + fileData.sampleNames.mkString("\t")
    val dataConcat = fileData.dataPairs.map(x => {x._1 + "\t" + x._2.toArray.mkString("\t")})

    bw.write(header + "\n")
    dataConcat.foreach(line => bw.write(line + "\n"))

    bw.flush()
    bw.close()
  }



}