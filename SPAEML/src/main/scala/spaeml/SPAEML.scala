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

import scala.annotation.tailrec

/*
 *  Having a different threshold for the forward and backward steps can lead to oscillations
 *    where an X is included under high p-value, then skipped, then included again, ... and this goes on forever
 */

object SPAEML extends Serializable {

  /**
    * Produce the full path of an object in AWS S3.
    *
    * @param s3BucketName The S3 bucket name
    * @param filePath The object's path inside S3 bucket
    * @return Full path of the S3 object with the proper prefix
    */
  def getFullS3Path(s3BucketName: String, filePath: String): String = {
    return "s3://" + s3BucketName + "/" + filePath
  }

  /**
    * Check if the output directory already exists, and delete the directory if it does.
    * @param spark The Spark session object
    * @param isOnAws Boolean indicating if the program is running on AWS
    * @param s3BucketName The S3 bucket name (only used if running on AWS)
    * @param outputDirectory The output directory's path
    */
  def clearOutputDirectory(spark: SparkSession,
                           isOnAws: Boolean,
                           s3BucketName: String,
                           outputDirectory: String
                          ): Unit = {

    val conf = spark.sparkContext.hadoopConfiguration

    val fs = if (isOnAws) {
      FileSystem.get(new URI("s3://" + s3BucketName), conf)
     } else {
      FileSystem.get(conf)
    }

    val outDirPath = if (isOnAws) {
      new Path(getFullS3Path(s3BucketName, outputDirectory))
    } else {
      new Path(outputDirectory)
    }

    if (fs.exists(outDirPath)) {
      fs.delete(outDirPath, true)
      println("Deleting output directory: " + outDirPath)
    }
  }

  /**
    * Verify if the output directory already exists. No side-effect.
    * @param spark The Spark session object
    * @param isOnAws Boolean indicating if the program is running on AWS
    * @param s3BucketName The S3 bucket name (only used if running on AWS)
    * @param outputDirectory The output directory's path
    * @return Boolean indicating if the output directory exists
    */
  def outputDirectoryAlreadyExists(spark: SparkSession,
                                   isOnAws: Boolean,
                                   s3BucketName: String,
                                   outputDirectory: String): Boolean = {

    val conf = spark.sparkContext.hadoopConfiguration

    val fs = if (isOnAws) {
      FileSystem.get(new URI("s3://" + s3BucketName), conf)
    } else {
      FileSystem.get(conf)
    }

    val outDirPath = if (isOnAws) {
      new Path(getFullS3Path(s3BucketName, outputDirectory))
    } else {
      new Path(outputDirectory)
    }

    return fs.exists(outDirPath)
  }

  /**
    * Write payload (String) to a file on HDFS (compatible with AWS S3).
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

    val fs = if (isOnAws) {
      FileSystem.get(new URI("s3://" + s3BucketName), conf)
    } else {
      FileSystem.get(conf)
    }

    val outputFilePath = if (isOnAws) {
      new Path(getFullS3Path(s3BucketName, new Path(outputDirectory, filename).toString))
    } else {
      new Path(outputDirectory, filename)
    }

    val writer = fs.create(outputFilePath)
    writer.writeChars(payload)
    writer.close()
  }

/*}

class SPAEML extends Serializable {
*/
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
    * 1. Broadcast the original SNP table throughout the cluster
    * 2. Create and distribute a Vector of columnName pairs for the SNP table
    * 3. On each Executor, create the SNP pairs for the columnName pairs on that Executor
    * 4. Perform all of the regressions in a Map and Reduce style
    */
  @tailrec
  final def performSteps(spark: SparkContext,
                   snpDataRDD: rdd.RDD[(String, DenseVector[Double])],
                   broadcastPheno: Broadcast[Map[String, DenseVector[Double]]],
                   phenoName: String,
                   collections: StepCollections,
                   threshold: Double,
                   prev_best_model: OLSRegression = null,
                   iterations: Int = 1
                  ): OLSRegression = {

    println("Iteration number: " + iterations.toString)
    /*
     *  LOCAL FUNCTIONS
     */

    /**
      * Returns the p-value associated with the newest term
      *
      *  This returns the second to last p-value of the input OLSRegression,
      *    as it assumes the last one is associated with the intercept
      */
    val getNewestTermsPValue = (reg: OLSRegression) => {
      // Drop the estimate of the intercept and return the p-value of the most recently added term
      reg.pValues.toArray.dropRight(1).last
    }

    val getNewestTermsName = (reg: OLSRegression) => reg.xColumnNames.last
    val getNewestTermsValues = (reg: OLSRegression) => reg.lastXColumnsValues().toDenseVector

    /**
      * Returns a OLSRegression object if the inputSnp is in the NotAdded category
      *
      *  Otherwise, an object of type None is returned (this SNP was not analyzed
      *    on this iteration)
      */
    def mapFunction(inputSnp: (String, DenseVector[Double])): Option[OLSRegression] = {

      // If the inputSnp is in the not_added category
      if ( collections.getNotAdded.contains(inputSnp._1) ) {
        val yVals = broadcastPheno.value(phenoName)
        val xColNames = collections.getAddedPrev.toArray

        val numRows = yVals.length
        val numCols = xColNames.length

        val xVals = xColNames.flatMap(collections.addedPrevValues(_))

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
      * Return the best model (non-deterministic when there are ties; as the tie is broken arbitrarily
      */
    def reduceFunction(inputRDD: rdd.RDD[Option[OLSRegression]]): OLSRegression = {
      val filtered = inputRDD.filter(x => x.isDefined).map(_.get)
      if (!filtered.isEmpty()) {
        filtered.reduce((x, y) => {
          if (getNewestTermsPValue(x) <= getNewestTermsPValue(y)) x else y
        })
      } else {
        // There are no more potential SNPs to be added
        throw new Exception("There are no more SNPs under consideration")
      }
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
    val bestRegression: OLSRegression = reduceFunction(mappedValues)

    bestRegression.printSummary()

    // If the p-value of the newest term does not meet the threshold, return the prev_best_model
    if (getNewestTermsPValue(bestRegression) >= threshold) {
      if (prev_best_model != null) { return prev_best_model }
      else { throw new Exception("No terms could be added to the model at a cutoff of " + threshold) }
    }
    else {
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
         *   (the items in the skipped category were skipped on this round, so we must create a final model without
         *   them included)
         */
        if (collections.getSkipped.isEmpty) return bestRegression
        else return rebuildModel()
      }
      else {
        /*
         * If any entries were skipped this round, recompute the regression without these terms,
         *  and then include the new best regression in the next iteration
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
    // Creates a Vector of all pairwise combinations of each column (names only, does not
    //   actually compute the new values: that will be done per partition)
    for (i <- columnNames.indices; j <- i + 1 until columnNames.length) yield {
      (columnNames(i), columnNames(j))
    }
  }

  protected def constructTimeString(startTime: Long, endTime: Long): String = {
    val seconds = (endTime - startTime) / 1e9
    val minutes = seconds / 60.0
    val hours = minutes / 60.0

    val getTimeString = (s: String, d: Double) => "Calculation time (in " + s + "): " + d.toString + "\n"
    val timeString = getTimeString("seconds", seconds) + getTimeString("minutes", minutes) + getTimeString("hours", hours)
    timeString
  }

  /**
    * Read in the genotype and phenotype files, perform SPAEML model building, and write the resulting model to a file
    *
    *   Detailed steps:
    *     1. Read in the data from the genotype and phenotype files (in the ".epiq" format, where the columns are
    *        sample names and the rows are SNP names/phenotype names)
    *
    *     2. Make sure the sample names match up exactly between the two files (error out if not)
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
    *     8. Initialize the StepCollections case class with all of the SNP_names put in the "not_added" category
    *
    *     9. For each phenotype, call the performSteps function
    *
    */
  def performSPAEML(spark: SparkSession,
                    epiqFileName: String,
                    pedFileName: String,
                    mapFileName: String,
                    phenotypeFileName: String,
                    outputDirectoryPath: String,
                    isOnAws: Boolean,
                    s3BucketName: String,
                    threshold: Double,
                    shouldSerialize: Boolean,
                    epistatic: Boolean
                 ) {

    val totalStartTime = System.nanoTime()

    if (SPAEML.outputDirectoryAlreadyExists(spark, isOnAws, s3BucketName, outputDirectoryPath)) {
      throw new Error("Output directory already exists. Either remove the directory or output to a different directory.")
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

    val phenotypeData = if (isOnAws) {
      readHDFSFile(SPAEML.getFullS3Path(s3BucketName, phenotypeFileName), spark.sparkContext)
    } else {
      readHDFSFile(phenotypeFileName, spark.sparkContext)
    }

    if (phenotypeData.sampleNames != snpData.sampleNames) {
      throw new Error("Sample order did not match between the SNP and Phenotype input files")
    }

    /*
     * From now on, the numbers are assumed to be in the same sample order between the genotypes and phenotypes
     * (In other words, it assumes the samples names A,B,C,... N from the two tables below are in the same order)
     *
     *   (Original genotype file)               (Original phenotype file)
     *
     *   Sample SNP1 SNP2 SNP3 ... SNPM         Sample  Pheno1  Pheno2 ...
     *   A      A1   A2   A3       AM           A       A_Ph1   A_Ph2
     *   B      B1   B2   B3       BM           B       B_Ph1   B_Ph2
     *   C      C1   C2   C3       CM           C       C_Ph1   C_Ph2
     *   ...                                    ...
     *   N      N1   N2   N3       NM           N       N_Ph1   N_PhN
     *
     *   Before this program is executed, the files are reformatted, and the columns and rows are transposed
     *   (this makes reading the data into Spark easier, i.e. we can read in one SNP/phenotype entry per line)
     *
     *   The reformatted (".epiq" formatted) files
     *      (A string called "Placeholder" is put in the top-left corner to make everything line up easily)
     *
     *   (genotype ".epiq" file)                  (phenotype ".epiq" file)
     *   Placeholder A   B   C  ... N           Placeholder A      B      C     ... N
     *   SNP1        A1  B1  C1     N1          Pheno1      A_Ph1  B_Ph1  C_Ph1     N_Ph1
     *   SNP2        A2  B2  C2     N2          Pheno2      A_Ph2  B_Ph2  C_Ph2     N_Ph2
     *   SNP3        A3  B3  C3     N3          ...
     *   ...
     *   SNPM        AM  BM  CM     NM
     */

    // Broadcast original SNP map where (SNP_name -> [SNP_values])
    val broadSnpTable: Broadcast[Map[String, DenseVector[Double]]] =
      spark.sparkContext.broadcast(snpData.dataPairs.toMap)

    val storageLevel = {
      if (shouldSerialize) StorageLevel.MEMORY_AND_DISK_SER
      else StorageLevel.MEMORY_AND_DISK
    }

    // Parallelize the original table into an RDD
    val singleSnpRDD = spark.sparkContext.parallelize(snpData.dataPairs)

    val fullSnpRDD = if (epistatic) {

      // Spread Vector of pairwise SNP_name combinations across cluster
      val pairwiseCombinations: Seq[(String, String)] = createPairwiseList(snpData.dataNames)
      val pairRDD = spark.sparkContext.parallelize(pairwiseCombinations)

      // Create the pairwise combinations across the cluster
      val pairedSnpRDD = pairRDD.map(createPairwiseColumn(_, broadSnpTable))

      (singleSnpRDD ++ pairedSnpRDD).persist(storageLevel)
    } else {
      singleSnpRDD.persist(storageLevel)
    }

    // Broadcast Phenotype map where (phenotype_name -> [values])
    val phenoBroadcast = spark.sparkContext.broadcast(phenotypeData.dataPairs.toMap)
    val phenotypeNames = phenotypeData.dataNames

    // The :_* unpacks the contents of the array as input to the hash set
    val snpNames: mutable.HashSet[String] = mutable.HashSet(fullSnpRDD.keys.collect(): _*)

    // Create the initial set of collections (the class that keeps track of which SNPs are in and out of the model)
    //   All SNPs start in the not_added category
    val initialCollections = new StepCollections(not_added = snpNames)

    /*
     *  For each phenotype, build a model, println and save the results
     */
    for (phenotype <- phenotypeNames) {

      val startTime = System.nanoTime()

      val bestReg = performSteps(spark.sparkContext, fullSnpRDD, phenoBroadcast,
                                 phenotype, initialCollections, threshold
                                )

      val endTime = System.nanoTime()

      val timeString = constructTimeString(startTime, endTime)
      val summaryString = bestReg.summaryString
      val anovaSummaryString = bestReg.anovaTable.summaryString

      // Include both the standard R-like regression output and the ANOVA table style output
      val genotypeFileNames = if (epiqFileName.isEmpty) {
        pedFileName + " " + mapFileName
      } else {
        epiqFileName
      }

      val output = Array(timeString,
                         "Genotype File: " + genotypeFileNames,
                         "Phenotype File: " + phenotypeFileName + "\n",
                         summaryString + "\n",
                         anovaSummaryString
                        )

      // Print
      output.foreach(println)

      // Save to file
      SPAEML.writeToOutputFile(
        spark, isOnAws, s3BucketName, outputDirectoryPath, phenotype + ".summary", output.mkString("\n")
      )

    }

    val totalEndTime = System.nanoTime()
    val totalTimeString = "\nTotal runtime (seconds): " + ((totalEndTime - totalStartTime) / 1e9).toString

    SPAEML.writeToOutputFile(spark, isOnAws, s3BucketName, outputDirectoryPath, "total_time.log", totalTimeString)
  }

}
