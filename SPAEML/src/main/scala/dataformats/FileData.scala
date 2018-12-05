package dataformats

import breeze.linalg.DenseVector

/**
  *
  * @param sampleNames The sample names
  * @param dataPairs tuples with (variant's name, the variant's values in the sample name's order)
  *
  *    Sample   VAR1  VAR2  VAR3 ...
  *    SAMPLE1  s1v1  s1v2  s1v3  ...    =====>   sampleNames = Vector(SAMPLE1, SAMPLE2, SAMPLE3, ...)
  *    SAMPLE2  s2v1  s2v2  s2v3  ...    =====>   dataPairs = Vector(
  *    SAMPLE3  s3v1  s3v2  s3v3  ...                                (VAR1, Vector(s1v1, s2v1, s3v1)),
  *    ...                                                           (VAR2, Vector(s1v2, s2v2, s3v2)),
  *                                                                  (VAR3, Vector(s1v3, s2v3, s3v3)),
  *                                                               ...
  *                                                              )
  */
class FileData(val sampleNames: Vector[String],
               val dataPairs: Vector[(String, DenseVector[Double])],
               val dataNames: Vector[String]
              ) {

  def this(sampleNames: Vector[String],
           dataPairs: Vector[(String, DenseVector[Double])]
          ) {
     this(sampleNames, dataPairs, dataNames = dataPairs.map(_._1))
  }

  def getFilteredFileData(SNPsToRemove: Vector[String]): FileData = {

    val filteredDataPairs = dataPairs.filter(x => !SNPsToRemove.contains(x._1))
    return new FileData(sampleNames, filteredDataPairs)
  }

}