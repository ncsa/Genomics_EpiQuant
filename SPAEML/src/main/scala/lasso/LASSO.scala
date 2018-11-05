package lasso

import breeze.linalg.DenseVector
import converters.PedMapParser
import dataformats.{FileData, LinearRegressionModel}
import loggers.EpiQuantLogger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import spaeml.SPAEML
import spaeml.SPAEML.readHDFSFile

object LASSO {

  def performLASSO(
                    spark: SparkSession,
                    epiqInputFile: String,
                    pedInputFile: String,
                    mapInputFile: String,
                    phenotypeInputFile: String,
                    outputDirectoryPath: String,
                    isOnAws: Boolean,
                    s3BucketName: String,
                    shouldSerialize: Boolean): Unit = {

    if (SPAEML.outputDirectoryAlreadyExists(spark, isOnAws, s3BucketName, outputDirectoryPath)) {
      EpiQuantLogger.error(
        "Output directory '" + outputDirectoryPath +
          "' already exists: Remove the directory or change the output directory location",
        new Error
      )
    }

    val genotypeData: FileData = {
      if (epiqInputFile.isEmpty) {
        new PedMapParser(mapInputFile, pedInputFile).fileData
      } else if (isOnAws) {
        readHDFSFile(SPAEML.getFullS3Path(s3BucketName, epiqInputFile), spark.sparkContext)
      } else {
        readHDFSFile(epiqInputFile, spark.sparkContext)
      }
    }

    val phenotypeData = {
      if (isOnAws) readHDFSFile(SPAEML.getFullS3Path(s3BucketName, phenotypeInputFile), spark.sparkContext)
      else readHDFSFile(phenotypeInputFile, spark.sparkContext)
    }

    val models = train(genotypeData, phenotypeData, spark)
    models.foreach(x => x.saveAsJSON(spark, isOnAws, s3BucketName, outputDirectoryPath, x.phenotypeName + ".lasso"))
  }

  def train(genotypeData: FileData, phenotypeData: FileData, spark: SparkSession): Vector[LinearRegressionModel] = {

    val output = Vector.newBuilder[LinearRegressionModel]

    for (phenotype <- phenotypeData.dataPairs) {

      val phenotype = phenotypeData.dataPairs(0)

      val rdd = createRDD(genotypeData, phenotype, spark.sparkContext)
      val model = LassoWithSGD.train(rdd, 100)
      val weights = genotypeData.dataNames zip model.weights.toArray

      output += new LinearRegressionModel(phenotype._1, weights, model.intercept)
    }

    output.result()
  }

  def createRDD(geno: FileData, pheno: (String, DenseVector[Double]), spark: SparkContext): RDD[LabeledPoint] = {

    val data = new ListBuffer[LabeledPoint]()

    for ((sample, index) <- geno.sampleNames.zipWithIndex) {

      val phenoPoint = pheno._2(index)
      val snpPoints = geno.dataPairs.map(x => x._2(index))
      val labledPoint = LabeledPoint(phenoPoint, Vectors.dense(snpPoints.toArray))

      data += labledPoint
    }

    spark.parallelize(data).cache()
  }

}