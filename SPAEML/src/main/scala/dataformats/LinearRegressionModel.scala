package dataformats

import java.net.URI
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import spaeml.SPAEML.getFullS3Path

class LinearRegressionModel(
                           val phenotypeName: String,
                           val SNPNames: Vector[String],
                           val weights: Vector[(String, Double)],
                           val intercept: Double) {

  def this(phenotypeName: String, weights: Vector[(String, Double)], intercept: Double) {
    this(phenotypeName, weights.map(_._1), weights, intercept)
  }

  // The names of the SNPs with zero coefficient
  lazy val SNPsToRemove = weights.filter(_._2 == 0).map(_._1)

  // The names of the SNPs with non-zero coefficients
  lazy val SNPsToKeep = weights.filter(_._2 != 0).map(_._1)

  override def toString(): String = {
    "Phenotype: " + phenotypeName + "\n" + "\tSNPs to remove: " + SNPsToRemove.mkString(", ") + "\n" + "\tSNPs to keep: " + SNPsToKeep.mkString(", ")
  }

  /**
    * Serialize the model into JSON and save to a file.
    * @param spark The configured Spark session
    * @param isOnAws Flag indicating if running on AWS
    * @param s3BucketName The S3 bucket name, if on AWS
    * @param outputDir The output directory to store the file
    * @param filename The filename
    */
  def saveAsJSON(spark: SparkSession, isOnAws: Boolean, s3BucketName: String, outputDir: String, filename: String): Unit = {

    val json: JObject = ("phenotype" -> this.phenotypeName) ~ ("weights" -> this.weights) ~ ("intercept" -> this.intercept)

    val conf = spark.sparkContext.hadoopConfiguration
    val fs: FileSystem = if (isOnAws) FileSystem.get(new URI("s3://" + s3BucketName), conf) else FileSystem.get(conf)

    val outputFilePath = {
      if (isOnAws) new Path(getFullS3Path(s3BucketName, new Path(outputDir, filename).toString))
      else new Path(outputDir, filename)
    }

    val writer = fs.create(outputFilePath)
    writer.writeUTF(pretty(render(json)))
    writer.close()
  }
}