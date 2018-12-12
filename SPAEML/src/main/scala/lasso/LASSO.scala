package lasso

import breeze.linalg.{DenseMatrix, DenseVector}
import converters.PedMapParser
import dataformats.{FileData, LinearRegressionModel}
import loggers.EpiQuantLogger
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.{LabeledPoint, LassoWithSGD}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import spaeml.SPAEML
import spaeml.SPAEML.readHDFSFile
import statistics.{ANOVATable, OLSRegression}

object LASSO {

  /**
    * Runs LASSO to build a model for each phenotype and output to JSON files.
    * @param spark The configured Spark session
    * @param epiqInputFile The .epiq file for genotype input data.
    * @param pedInputFile The .ped file for genotype input data, must be paired with .map input.
    * @param mapInputFile The .map file for genotype input data, must be paried with .ped input.
    * @param phenotypeInputFile The file for phenotype input data.
    * @param outputDirectoryPath The output directory.
    * @param isOnAws Flag indicating if running on AWS.
    * @param s3BucketName The 3S bucket name for input and output files, if on AWS.
    */
  def performLASSO(
                    spark: SparkSession,
                    epiqInputFile: String,
                    pedInputFile: String,
                    mapInputFile: String,
                    phenotypeInputFile: String,
                    outputDirectoryPath: String,
                    isOnAws: Boolean,
                    s3BucketName: String): Unit = {

    if (SPAEML.outputDirectoryAlreadyExists(spark, isOnAws, s3BucketName, outputDirectoryPath)) {
      EpiQuantLogger.error(
        "Output directory '" + outputDirectoryPath +
          "' already exists: Remove the directory or change the output directory location"
      )
      throw new Error("Output directory already exists")
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

    models.foreach(x => x._2.saveAsJSON(spark, isOnAws, s3BucketName, outputDirectoryPath, x._2.phenotypeName + ".json"))
    produceAndSaveANOVATables(genotypeData, phenotypeData, models, spark, outputDirectoryPath, isOnAws, s3BucketName)
  }

  /**
    * Train LASSO models using Spark's MLLib.
    * @param genotypeData The genotype input data as FileData
    * @param phenotypeData The phenotype input data as FileData
    * @param spark The configured Spark session
    * @return A map from phenotype names to resulting LinearRegressionModels
    */
  def train(genotypeData: FileData, phenotypeData: FileData, spark: SparkSession): Map[String, LinearRegressionModel] = {

    val output = Map[String, LinearRegressionModel]()

    for (phenotype <- phenotypeData.dataPairs) {

      EpiQuantLogger.info("Running LASSO on phenotype " + phenotype._1)

      val rdd = createRDD(genotypeData, phenotype, spark.sparkContext)
      val model = LassoWithSGD.train(rdd, 100)
      val weights = genotypeData.dataNames zip model.weights.toArray

      val resultModel = new LinearRegressionModel(phenotype._1, weights, model.intercept)
      output += (phenotype._1 -> resultModel)

      EpiQuantLogger.info("Finished running LASSO on phenotype " + phenotype._1)
      EpiQuantLogger.info(resultModel)
    }

    return output
  }

  /**
    * Create a RDD for LASSO input.
    * @param geno The genotype input data.
    * @param pheno The phenotype data (phenotype name, phenotype values for all samples)
    * @param spark The configured Spark session
    * @return A RDD of LabeledPoint consisting of the input data, ready to feed into LASSO.
    */
  def createRDD(geno: FileData, pheno: (String, DenseVector[Double]), spark: SparkContext): RDD[LabeledPoint] = {

    val data = new ListBuffer[LabeledPoint]()

    for ((_, index) <- geno.sampleNames.zipWithIndex) {

      val phenoPoint = pheno._2(index)
      val snpPoints = geno.dataPairs.map(x => x._2(index))
      val labledPoint = LabeledPoint(phenoPoint, Vectors.dense(snpPoints.toArray))

      data += labledPoint
    }
    spark.parallelize(data).cache()
  }

  def produceAndSaveANOVATables(
                                genotypeData: FileData,
                                phenotypeData: FileData,
                                models: Map[String, LinearRegressionModel],
                                spark: SparkSession,
                                outputDir: String,
                                isOnAws: Boolean,
                                s3BucketName: String): Unit = {

    EpiQuantLogger.info("Logging and saving LASSO results")

    for (phenotype <- phenotypeData.dataPairs) {

      val table = produceANOVATable(genotypeData, phenotype, models(phenotype._1))
      EpiQuantLogger.info(table.summaryStrings.mkString("\n"))
      SPAEML.writeToOutputFile(spark, isOnAws, s3BucketName, outputDir, phenotype._1 + ".summary", table.summaryStrings.mkString("\n"))
    }
  }

  /**
    * Create a ANOVA table from given data and LASSO linear regression model.
    * Run OLS regression once on all SNPs with non-zero coefficients and produce ANOVA table.
    * @param genotypeData: the full genotype data
    * @param phenotypeData: the full phenotype data
    * @param model: the linear regression model produced by LASSO
    * @return: the resulting ANOVA table
    */
  def produceANOVATable(
                         genotype: FileData,
                         phenotype: (String, DenseVector[Double]),
                         model: LinearRegressionModel): ANOVATable = {

    val filteredGenotypeData = genotype.getFilteredFileData(model.SNPsToRemove)

    val xColumnNames = filteredGenotypeData.dataNames.toArray
    val yColumnName = phenotype._1

    val numRows = filteredGenotypeData.sampleNames.length
    val numCols = filteredGenotypeData.dataNames.length
    val xValues = filteredGenotypeData.dataPairs.map(_._2.toArray).toArray.flatten
    val Xs = new DenseMatrix(numRows, numCols, xValues)

    val Y = phenotype._2

    val regression = new OLSRegression(xColumnNames, yColumnName, Xs, Y)
    new ANOVATable(regression)
  }

}