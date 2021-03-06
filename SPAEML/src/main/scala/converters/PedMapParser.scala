package converters
import breeze.linalg.DenseVector
import scala.io.Source
import scala.collection.immutable.Map
import dataformats.FileData

/**
  * This class is a parser for converting standard PLINK (.map & .ped) files
  * into either an intermediate .epiq file or directly into a FileData
  * object to be used in SPAEML.
  *
  * MAP file format:
  * Columns:
  *   4 columns:
  *     - chromosome # (int)
  *     - SNP ID (string)
  *     - SNP genetic position (float)
  *     - SNP physical position (int)
  * Rows:
  *   L rows (L = # total SNPs), one row for each SNP
  *
  * PED file format:
  * Columns:
  *   first 6 columns:
  *     - family ID (string, unique)
  *     - individual ID (string, unique within family)
  *     - father ID (string)
  *     - mother ID (string)
  *     - sex (int)
  *     - phenotype (float)]
  *   followed by 2*L columns:
  *     - 1st allele of 1st SNP
  *     - 2nd allele of 1st SNP
  *     - ...
  *     - 1st allele of L'th SNP
  *     - 2nd allele of L'th SNP
  * Rows:
  *   N rows (N = # total individuals), one row for each individual
  *
  * @param mapFilePath Path to the .map file
  * @param pedFilePath Path to the .ped file
  * @param delimiter The delimiter used by .map and .ped file (default is a single space)
  */

class PedMapParser(mapFilePath: String, pedFilePath: String, delimiter: String=" ") extends FileParser {

  // The index of the column for SNP names in a .map file
  private val SNP_NAME_COLUMN_POS = 1

  // List storing all SNP names
  val SNPs: Stream[String] =
    Source.fromFile(mapFilePath, FILE_ENCODING).getLines().map(_.split(delimiter)(SNP_NAME_COLUMN_POS)).toStream
  // Matrix storing all data from the .ped file
  val PEDs: Stream[Array[String]] =
    Source.fromFile(pedFilePath, FILE_ENCODING).getLines().map(_.split(delimiter)).toStream
  // List storing all sample names
  val sampleNames: Stream[String] = PEDs.map(x => {x(0) + x(1)})

  /**
    * Parse the .ped and .map files and return the result as a FileData object.
    */
  val fileData: FileData = {
    new FileData(
      sampleNames=sampleNames.toVector,
      dataPairs=encodeSamplesForAllSNPs(PEDs, getAllMajorAndMinorAlleles(PEDs, SNPs))
    )
  }

  /**
    * Get the major and minor alleles for each SNP.
    * @param pedMatrix The PED matrix
    * @param snpArray A list storing all SNP names
    * @return A vector storing (SNP name, major allele, minor allele) for each SNP
    */
  private def getAllMajorAndMinorAlleles(
                                          pedMatrix: Stream[Array[String]],
                                          snpArray: Stream[String]): Vector[(String, String, String)] = {

    val output = Vector.newBuilder[(String, String, String)] // Format: (SNP name, major allele, minor allele)

    for ((snpName, index) <- snpArray.zipWithIndex) {

      // For each SNP, get all occurrence of alleles (concatenating elements of the two corresponding columns for the SNP)
      val allelesOfSNP: Stream[String] = pedMatrix.map(_(5 + index*2 + 1)) ++ pedMatrix.map(_(5 + index*2 + 2))

      // Turn the list into a counter (mapping from unique allele to number of occurrences)
      var allelesCount: Map[String, Int] = allelesOfSNP.foldLeft(Map.empty[String, Int].withDefaultValue(0)) {
        (map, element) => map + (element -> (map(element) + 1))
      }

      // Get the name (_1) of the max element in the map by occurrence count (_2)
      val major = allelesCount.maxBy(_._2)._1

      // Drop the major allele from the map
      allelesCount -= major

      // If there is only one allele for the SNP, then set minor to empty, otherwise, use maxBy again to find minor allele.
      val minor = if (allelesCount.isEmpty) "" else allelesCount.maxBy(_._2)._1

      // Append to the output vector
      output += ((snpName, major, minor))
    }

    output.result()
  }

  /**
    * Encode all data pairs for each sample for each SNP.
    * @param pedMatrix The PED matrix
    * @param snpArray A list storing (SNP name, major allele, minor allele) for each SNP
    * @return A vector storing (SNP name, vector of encoded values for each sample) for each SNP
    */
  private def encodeSamplesForAllSNPs(pedMatrix: Stream[Array[String]],
                                      snpArray: Vector[(String, String, String)]
                                     ): Vector[(String, DenseVector[Double])] = {

    val output = Vector.newBuilder[(String, DenseVector[Double])]

    for (((snpName, major, minor), index) <- snpArray.zipWithIndex) {

      val snpPairs: Stream[(String, String)] = pedMatrix.map(x => {(x(5 + index*2 + 1), x(5 + index*2 + 2))})
      val encodedSNP: Stream[Double] = snpPairs.map(x => {encodeSNP(x._1, x._2, major, minor)})
      output += ((snpName, new DenseVector[Double](encodedSNP.toArray)))
    }

    output.result()
  }

  /**
    * Given a pair of SNP and the major/minor alleles, encode the pair into a single value.
    *
    * Assuming C is the minor allele, it will recode genotypes as follows:
    *
    * SNP       SNP_A ,  SNP_HET
    * ---       -----    -----
    * A A   ->    0   ,   0
    * A C   ->    1   ,   1
    * C C   ->    2   ,   0
    * 0 0   ->   NA   ,  NA
    *
    * Note: we want to use the equivalent of PLINK's recodeA, because it omits the SNP_HET info.
    * Note: when an allele that is both non-major and non-minor is involved, the corresponding value is NULL.
    *
    * @param firstAllele One allele in the pair
    * @param secondAllele The other allele in the pair
    * @param majorAllele The major allele for the SNP in population
    * @param minorAllele The minor allele for the SNP in population
    * @return A value (0, 1, or 2) encoding the SNP pair information
    */
  private def encodeSNP(
                         firstAllele: String,
                         secondAllele: String,
                         majorAllele: String,
                         minorAllele: String): Double = {

    val alleles = List(majorAllele, minorAllele)

    if (!alleles.contains(firstAllele) || !alleles.contains(secondAllele)) {
      Double.NaN
    }

    if (firstAllele == secondAllele) {
      if (firstAllele == majorAllele) 0.0 else 2.0
    } else {
      1.0
    }
  }

}