package spaeml

import statistics._
import java.net.URI
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast._
import breeze.linalg.{DenseMatrix, DenseVector}
import converters.PedMapParser
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import dataformats.FileData
import loggers.EpiQuantLogger
import scala.annotation.tailrec
import lasso.LASSO

object SPAEML extends Serializable {

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

  // Implicit function needed for the "flatten" method to work on a DenseVector
  implicit val DenseVector2ScalaVector: DenseVector[Double] => Vector[Double] =
    (s: DenseVector[Double]) => s.toScalaVector

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

  def createPairwiseColumn(pair: (String, String),
                                   broadMap: Broadcast[Map[String, DenseVector[Double]]]
                                  ): (String, DenseVector[Double]) = {
    val combinedName = pair._1 + "_" + pair._2
    val firstVals = broadMap.value(pair._1)
    val secondVals = broadMap.value(pair._2)
    val newVals = for (i <- 0 until firstVals.size) yield firstVals(i) * secondVals(i)
    (combinedName, DenseVector(newVals:_*))
  }

  /**
    * Construct a regression model associating the phenotype with the genotype data using the SPAEML algorithm
    *
    * SPAEML procedure:
    *
    *  Forward Step:
    *    1. For each variant (SNP) not included in the model, create a new model with the variant included
    *    2. Out of all of the models that were created, select the model where the newly added term best fits the
    *         selection criterion (e.g. has the lowest p-value)
    *    3. Verify that the newest term's selection criterion meets the threshold (e.g. the newest terms p-value is
    *         below the p-value threshold)
    *  Backward Step:
    *    4. Remove any terms that were added to the model previously that no longer meet the threshold
    *  Repeat Steps:
    *    5. Repeat this procedure until
    *        a). All possible terms have been included in the model
    *        b). The newest term's selection criterion does not meet the threshold (in this case, return the previous
    *            iterations best model)
    *
    * @param spark the running SparkContext
    * @param snpDataRDD key-value RDD where the name of each variant points to their values for each sample
    * @param broadcastPheno broadcasted map of the phenotype data where each phenotype name points their values
    * @param phenoName name of the phenotype used as the response variable in the model
    * @param collections an object storing the state of the model after previous iterations
    * @param threshold the p-value threshold used while model building
    * @param prevBestModel previous iteration's best model (returned if the new best model doesn't meet the threshold)
    * @param iterations counter tracking how many iterations have passed
    *
    * @return the regression model produced by applying the SPAEML algorithm to the genotype/phenotype data
    */
  @tailrec
  final def performSteps(spark: SparkContext,
                         snpDataRDD: rdd.RDD[(String, DenseVector[Double])],
                         broadcastPheno: Broadcast[Map[String, DenseVector[Double]]],
                         phenoName: String,
                         collections: StepCollections,
                         threshold: Double,
                         prevBestModel: OLSRegression = null,
                         iterations: Int = 1
                         ): Option[OLSRegression] = {
    EpiQuantLogger.info("SPAEML model-building iteration number: " + iterations.toString)

    /*
     *  LOCAL FUNCTIONS
     */

    /**
      * Returns the p-value associated with the newest term
      *
      *  This returns the second to last p-value of the input OLSRegression,
      *    as it assumes the last one is associated with the intercept
      */
    def getNewestTermsPValue(reg: OLSRegression): Double = {
      // Drop the estimate of the intercept and return the p-value of the most recently added term
      reg.pValues.toArray.dropRight(1).last
    }

    def getNewestTermsName(reg: OLSRegression): String = reg.xColumnNames.last
    def getNewestTermsValues(reg: OLSRegression): DenseVector[Double] = reg.lastXColumnsValues().toDenseVector

    /**
      * Returns a OLSRegression object if the inputSnp is in the NotAdded category
      *
      *  Otherwise, an object of type None is returned (this SNP was not analyzed
      *    on this iteration)
      */
    def mapFunction(inputSnp: (String, DenseVector[Double])): Option[OLSRegression] = {
      // If the inputSnp is in the not_added category
      if ( collections.getNotAdded.contains(inputSnp._1) ) {

        val xColNames = collections.getAddedPrev.toArray

        val xVals = xColNames.flatMap(collections.addedPrevValues(_))
        val yVals = broadcastPheno.value(phenoName)

        val numRows = yVals.length
        val numCols = xColNames.length

        val newXColNames = xColNames :+ inputSnp._1
        val newXVals: Array[Double] = xVals ++ inputSnp._2
        val newXValsAsMatrix = new DenseMatrix(numRows, numCols + 1, newXVals)

        return Some(new OLSRegression(newXColNames, phenoName, newXValsAsMatrix, yVals))
      } else {
        // Do not analyze this SNP
        return None
      }
    }

    /**
      * Return the best model in the RDD
      *
      *   non-deterministic when there are ties; as the tie is broken arbitrarily
      *
      * @param inputRDD the inputRDD that is the result of the mapFunction
      * @return the best OLS Regression from the input RDD
      */
    def reduceFunction(inputRDD: rdd.RDD[Option[OLSRegression]]): OLSRegression = {
      // Remove variants that were already included in the model, and were therefore skipped by the map function
      val filtered = inputRDD.filter(x => x.isDefined).map(_.get)
      filtered.reduce( (x, y) => if (getNewestTermsPValue(x) <= getNewestTermsPValue(y)) x else y )
    }

    /**
      *  Returns a new model built with the current entries in the collections.added_prev category
      *  (because collections has mutable state, this can be used to recreate the model after a change to collections
      *   has been made)
      */
    def rebuildModel(): OLSRegression = {
      val xNames: Array[String] = collections.getAddedPrev.toArray
      val xVals: Array[Double] = xNames.flatMap(collections.addedPrevValues(_).toScalaVector())
      val yVals = broadcastPheno.value(phenoName)

      val numRows = yVals.length
      val numCols = xNames.length

      val xValsMatrix: DenseMatrix[Double] = new DenseMatrix(numRows, numCols, xVals)
      new OLSRegression(xNames, phenoName, xValsMatrix, yVals)
    }
    /*
     * IMPLEMENTATION
     */

    /*==================================================================================================================
     *  Step 1: find the best regression for those SNPs still under consideration
     =================================================================================================================*/

    // Map generates all of the regression outputs, and reduce finds the best one
    val mappedValues: rdd.RDD[Option[OLSRegression]] = snpDataRDD.map(mapFunction)

    // Reduce returns the best regression
    val bestRegression: OLSRegression = reduceFunction(mappedValues)

    bestRegression.logSummary()

    // If the p-value of the newest term does not meet the threshold, return the prevBestModel
    if (getNewestTermsPValue(bestRegression) >= threshold) {
      if (prevBestModel == null) {
        EpiQuantLogger.warn(
          "No variants could be added to the model with the " + phenoName +
            " phenotype: the threshold of " + threshold + " may be too strict"
        )
        return None
      }
      return Option(prevBestModel)

    } else {
      /*
       * Now that the regressions for this round have been completed, return any entries in the skipped category to the
       *   not added category
       */
      collections.getSkipped.foreach( x => collections.moveFromSkipped2NotAdded(x) )

      // Remove the newest term from the not_added category and put it in the added_prev category
      collections.moveFromNotAdded2AddedPrev(snpName = getNewestTermsName(bestRegression),
        snpValues = getNewestTermsValues(bestRegression)
      )
      /*================================================================================================================
       * Step 2: Check to make sure none of the previously added terms are no longer significant
       * 				 If they are, remove them from consideration in the next round (put them in the skipped category) and
       *         take them out of the model
       ===============================================================================================================*/

      val namePValuePairs: Array[(String, Double)] = bestRegression.xColumnNames.zip(bestRegression.pValues)

      // Remove all terms that are no longer significant, and move them to the skipped category
      namePValuePairs.foreach(pair => if (pair._2 >= threshold) collections.moveFromAddedPrev2Skipped(pair._1) )

      if (collections.getNotAdded.isEmpty) {
        /*
         * No more terms are under consideration. Return the current best model, unless there are entries
         * in the skipped category.
         *
         * If this is the case, perform one last regression with the current collections.add_prev terms in the model
         *   (the items in the skipped category were skipped on this round, so we must create a final model with those
         *   terms excluded)
         */
        EpiQuantLogger.warn("There are no more variants that could be added to the model: " +
          "the threshold of " + threshold + " may be too lenient"
        )
        if (collections.getSkipped.isEmpty) return Option(bestRegression)
        else return Option(rebuildModel())
      }
      else {
        /*
         * If any entries were skipped this round, recompute the regression without these terms,
         *  and include the new best regression in the next iteration
         */
        val newBestReg: OLSRegression =  if (collections.getSkipped.nonEmpty) rebuildModel() else bestRegression

        /*==============================================================================================================
         * Step 3: Make the recursive call with the updated information
         =============================================================================================================*/
        performSteps(spark, snpDataRDD, broadcastPheno, phenoName, collections, threshold, newBestReg, iterations + 1)
      }
    }
  }

  protected def flattenArrayOfBreezeVectors(input: Array[breeze.linalg.Vector[Double]]): Array[Double] = {
    input.flatMap(breezeVector => breezeVector.toDenseVector.toScalaVector)
  }

  /**
   * Create a non-redundant pairwise Vector of names from a vector of string inputs
   *
   *  Non-redundant means that X_Y is the same as Y_X, and we only create one
   *    of the two with this function
   */
  def createPairwiseList(columnNames: Seq[String]): Seq[(String, String)] = {
    // Creates a Vector of all pairwise combinations of each column name
    for (i <- columnNames.indices; j <- i + 1 until columnNames.length) yield {
      (columnNames(i), columnNames(j))
    }
  }

  /**
    * Returns a set of strings that represent how much time has elapsed
    *
    * The strings are formatted: "Calculation time (in <seconds | minutes | hours>): <time>"
    *
    * @param startTime The start time in nanoseconds since the current epoch
    * @param endTime The end time in nanoseconds since the current epoch
    * @return A vector with the elapsed time in seconds, minutes, and hours
    */
  private def constructTimeStrings(startTime: Long, endTime: Long): Vector[String] = {
    val seconds = (endTime - startTime) / 1e9
    val minutes = seconds / 60.0
    val hours = minutes / 60.0

    val getTimeString = (s: String, d: Double) => "Calculation time (in " + s + "): " + f"$d%1.5f"
    Vector(getTimeString("seconds", seconds),  getTimeString("minutes", minutes), getTimeString("hours", hours))
  }

  /**
    * Read in the genotype and phenotype files, perform SPAEML model building, and write the resulting model to a file
    *
    *   Detailed steps:
    *     1. Read in the genotype/phenotype data
    *
    *     2. Ensure the sample names match up between the two files
    *     3. broadcast (send a read-only copy of) the original SNP data map (SNP_name -> [values]) to each executor
    *
    *     4. On the driver, compute all of the non-redundant pairwise combinations of the SNP_names, and spread those
    *          name pairs across the cluster in an RDD
    *
    *     5. Across the cluster, compute the pairwise SNP combination RDD by using the SNP_name pair RDD and the data
    *          from the SNP table that was broadcast to all of the executors (combined names are named <SNP_A>_<SNP_B>)
    *
    *     6. Spread the original SNP data table across the cluster as an RDD, then combine the original SNP RDD with
    *          the pairwise SNP to create the fullSnpRDD (and set it to persist with the desired serialization level)
    *
    *     7. Broadcast the phenotype data map (Phenotype_name -> [values]) to each executor
    *     8. Initialize the StepCollections class with all of the SNP_names placed in the "not_added" category
    *
    *     9. For each phenotype, call the performSteps function
    *
    * @param spark the running SparkSession instance
    * @param epiqFileName path of the epiq-formated genotype file (empty if input not in epiq format)
    * @param pedFileName path of the ped-formated genotype file (empty if input the ped/map format is not used)
    * @param mapFileName path of the map-formated genotype file (empty if input the ped/map format is not used)
    * @param phenotypeFileName path to the epiq-formated phenotype file
    * @param outputDir path of the output directory
    * @param isOnAws is the program being run on AWS
    * @param s3BucketName the AWS S3 bucket (if AWS is being used)
    * @param threshold the p-value threshold used during SPAEML model building
    * @param epistatic consider pairwise combinations of genotypic terms during model building
    * @param shouldSerialize should data be stored serially (reduced memory footprint; longer reading time)
    */
  def performSPAEML(spark: SparkSession,
                    epiqFileName: String,
                    pedFileName: String,
                    mapFileName: String,
                    phenotypeFileName: String,
                    outputDir: String,
                    isOnAws: Boolean,
                    s3BucketName: String,
                    threshold: Double,
                    epistatic: Boolean,
                    runLasso: Boolean,
                    shouldSerialize: Boolean
                   ) {

    val totalStartTime = System.nanoTime()

    if (SPAEML.outputDirectoryAlreadyExists(spark, isOnAws, s3BucketName, outputDir)) {
      EpiQuantLogger.error(
        "Output directory '" + outputDir +
        "' already exists: Remove the directory or change the output directory location",
        new Error
      )
    }

    val snpData: FileData = {
      if (epiqFileName.isEmpty) {
        new PedMapParser(mapFileName, pedFileName).fileData
      } else if (isOnAws) {
        readHDFSFile(SPAEML.getFullS3Path(s3BucketName, epiqFileName), spark.sparkContext)
      } else {
        readHDFSFile(epiqFileName, spark.sparkContext)
      }
    }

    val phenotypeData = {
      if (isOnAws) readHDFSFile(SPAEML.getFullS3Path(s3BucketName, phenotypeFileName), spark.sparkContext)
      else readHDFSFile(phenotypeFileName, spark.sparkContext)
    }

    if (phenotypeData.sampleNames != snpData.sampleNames) {
      EpiQuantLogger.error("Sample order did not match between the SNP and Phenotype input files", new Error)
    }

    val storageLevel = if (shouldSerialize) StorageLevel.MEMORY_AND_DISK_SER else StorageLevel.MEMORY_AND_DISK

    // Lazily load LASSO model for future steps in case runLasso is set to true
    lazy val lassoModels = LASSO.train(snpData, phenotypeData, spark)

    EpiQuantLogger.debug("Creating original variant broadcast table")
    // Broadcast original SNP map where (SNP_name -> [SNP_values])
    val broadSnpTable: Broadcast[Map[String, DenseVector[Double]]] =
      spark.sparkContext.broadcast(snpData.dataPairs.toMap)
    EpiQuantLogger.info("Created original variant broadcast table")

    // Broadcast Phenotype map where (phenotype_name -> [values])
    EpiQuantLogger.debug("Broadcasting phenotype value lookup table")
    val phenoBroadcast = spark.sparkContext.broadcast(phenotypeData.dataPairs.toMap)
    EpiQuantLogger.info("Broadcasted phenotype value lookup table")

    val phenotypeNames = phenotypeData.dataNames

    //  For each phenotype, build a model, log and save the results
    for (phenotype <- phenotypeNames) {

      val startTime = System.nanoTime()

      val filteredSnpData= if (runLasso) {
        val lassoModel = lassoModels(phenotype)
        snpData.getFilteredFileData(lassoModel.SNPsToRemove)
      } else {
        snpData
      }

      EpiQuantLogger.debug("Creating original variant RDD")
      // Parallelize the original table into an RDD
      val singleSnpRDD = spark.sparkContext.parallelize(filteredSnpData.dataPairs)
      EpiQuantLogger.info("Created original variant RDD")

      val fullSnpRDD: rdd.RDD[(String, DenseVector[Double])] = {
        if (epistatic) {
          // Spread Vector of pairwise SNP_name combinations across cluster
          val pairwiseCombinations: Seq[(String, String)] = createPairwiseList(filteredSnpData.dataNames)
          EpiQuantLogger.debug("Created pairwise variant-name combinations")

          EpiQuantLogger.debug("Creating pairwise variant RDD")
          val pairRDD = spark.sparkContext.parallelize(pairwiseCombinations)
          EpiQuantLogger.info("Created pairwise variant RDD")

          // Create the pairwise combinations across the cluster
          val pairedSnpRDD = pairRDD.map(createPairwiseColumn(_, broadSnpTable))
          EpiQuantLogger.debug("Spread pairwise variant-name combinations across cluster")

          EpiQuantLogger.debug("Combining pairwise and original variant RDDs")
          val combinedRDD = (singleSnpRDD ++ pairedSnpRDD).persist(storageLevel)
          EpiQuantLogger.info("Combined pairwise and original variant RDDs")
          combinedRDD
        } else {
          singleSnpRDD.persist(storageLevel)
        }
      }

      // The :_* unpacks the contents of the array as input to the hash set
      val snpNames: mutable.HashSet[String] = mutable.HashSet(fullSnpRDD.keys.collect(): _*)

      // Create the initial set of collections (the class that keeps track of which SNPs are in and out of the model)
      //   All SNPs start in the not_added category
      val initCollections = new StepCollections(not_added = snpNames)

      // Build the model for the given phenotype
      val performStepsOutput: Option[OLSRegression] =
        performSteps(spark.sparkContext, fullSnpRDD, phenoBroadcast, phenotype, initCollections, threshold)

      val endTime = System.nanoTime()

      // Time the current model building process
      val timeStrings: Vector[String] = constructTimeStrings(startTime, endTime)

      // Generate the output string (either an ANOVA table or a statement that no model could be built)
      val modelOutputString: String = {
        performStepsOutput match {
          case Some(model) => {
            // Create the ANOVA summary table
            val anovaTable = new ANOVATable(model)
            anovaTable.summaryStrings.mkString("\n")
          }
          case _ => "No variants could be added to the model at the threshold of " + threshold
        }
      }

      val genotypeFileNames = if (epiqFileName.isEmpty) pedFileName + ", " + mapFileName else epiqFileName

      val output: Vector[String] = timeStrings ++
        Vector("Genotype File(s): " + genotypeFileNames, "Phenotype File: " + phenotypeFileName, modelOutputString)

      // Log the final model built
      output.foreach(EpiQuantLogger.info(_))

      // Save to file
      SPAEML.writeToOutputFile(spark, isOnAws, s3BucketName, outputDir, phenotype + ".summary", output.mkString("\n"))
    }

    val totalEndTime = System.nanoTime()

    // Save the overall run time
    val totalTimeString: String = constructTimeStrings(totalStartTime, totalEndTime).mkString("\n")

    SPAEML.writeToOutputFile(spark, isOnAws, s3BucketName, outputDir, "total_time.log", totalTimeString)
  }

}
