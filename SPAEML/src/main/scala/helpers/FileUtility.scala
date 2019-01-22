package helpers

import java.net.URI

import breeze.linalg.DenseVector
import dataformats.FileData
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object FileUtility {

  /**
    * Produce the full path of an object in AWS S3.
    *
    * @param s3BucketName The S3 bucket name
    * @param filePath The object's path inside S3 bucket
    * @return Full path of the S3 object with the proper prefix
    */
  def getFullS3Path(s3BucketName: String, filePath: String): String = {
    "s3://" + s3BucketName + "/" + filePath
  }

  /**
    * Verify if the output directory already exists. No side-effect.
    *
    * @param spark The Spark session object
    * @param isOnAws Boolean indicating if the program is running on AWS
    * @param s3BucketName The S3 bucket name (only used if running on AWS)
    * @param outputDirectory The output directory's path
    * @return Boolean indicating if the output directory exists
    */
  def outputDirectoryAlreadyExists(spark: SparkSession,
                                   isOnAws: Boolean,
                                   s3BucketName: String,
                                   outputDirectory: String
                                  ): Boolean = {
    val conf = spark.sparkContext.hadoopConfiguration

    val fs = if (isOnAws) FileSystem.get(new URI("s3://" + s3BucketName), conf) else FileSystem.get(conf)
    val outDirPath = if (isOnAws) new Path(getFullS3Path(s3BucketName, outputDirectory)) else new Path(outputDirectory)

    fs.exists(outDirPath)
  }

  /**
    * Write payload (String) to a file on HDFS (compatible with AWS S3).
    *
    * @param spark The Spark session object
    * @param isOnAws Boolean indicating if the program is running on AWS
    * @param s3BucketName The S3 bucket name (only used if running on AWS)
    * @param outputDirectory The output directory's path
    * @param filename The output file's name
    * @param payload The content (String) to write to the file
    */
  def writeToOutputFile(spark: SparkSession,
                        isOnAws: Boolean,
                        s3BucketName: String,
                        outputDirectory: String,
                        filename: String,
                        payload: String
                       ): Unit = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs: FileSystem = if (isOnAws) FileSystem.get(new URI("s3://" + s3BucketName), conf) else FileSystem.get(conf)

    val outputFilePath = {
      if (isOnAws) new Path(getFullS3Path(s3BucketName, new Path(outputDirectory, filename).toString))
      else new Path(outputDirectory, filename)
    }

    val writer = fs.create(outputFilePath)

    writer.writeUTF(payload)
    writer.close()
  }

  /** Reads in a file from HDFS converted previously with the ConvertFormat tool */
  def readHDFSFile(filePath: String, spark: SparkContext): FileData = {
    val splitLines = spark.textFile(filePath).map(_.split("\t").toVector)
    val headerLine = splitLines.filter(x => x(0) == "HeaderLine" || x(0) == "Headerline")
      .collect
      .flatten
      .toVector
    val dataLines = splitLines.filter(x => x(0) != "HeaderLine" && x(0) != "Headerline")
      .collect
      .toVector
    // Turns each data line into a tuple where (sampleName, DenseVector[values])
    // Drops the first column because that is the SNP name
    // The :_* unpacks the collection's value to be passed to the DenseVector's constructor one at a time
    val dataTuples = dataLines.map(x => {
      Tuple2(x(0),
        DenseVector( x.drop(1).map(_.toDouble):_* )
      )
    })
    new FileData(sampleNames = headerLine.drop(1), dataPairs = dataTuples)
  }

}
