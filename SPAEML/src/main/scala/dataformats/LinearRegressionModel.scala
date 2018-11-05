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