package spaeml

import statistics._
import scala.annotation.tailrec
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.broadcast._
import breeze.linalg.DenseVector
import breeze.linalg.{Vector => BreezeVector}
import breeze.linalg.DenseMatrix

class DenseFileData(sampleNames: Vector[String], dataPairs: Vector[(String, DenseVector[Double])])
  extends FileData(sampleNames, dataPairs)

class SPAEMLDense {}

object SPAEMLDense extends SPAEML {

  // Implicit function needed for the "flatten" method to work on a DenseVector
  implicit val DenseVector2ScalaVector: DenseVector[Double] => Vector[Double] =
    (s: DenseVector[Double]) => s.toScalaVector
  
  /** Reads in a file from HDFS converted previously with the ConvertFormat tool */
  def readHDFSFile(filePath: String, spark: SparkContext): DenseFileData = {
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
    new DenseFileData(sampleNames = headerLine.drop(1), dataPairs = dataTuples)
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
  def performSteps(spark: SparkContext,
                   snpDataRDD: rdd.RDD[(String, DenseVector[Double])],
                   broadcastPheno: Broadcast[Map[String, DenseVector[Double]]],
                   phenoName: String,
                   collections: StepCollections,
                   threshold: Double,
                   prev_best_model: OLSRegression = null, // Initialized to null in super trait
                   iterations: Int = 1 // Initialized to 1 in super trait
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
    def mapFunction(inputSnp: (String, DenseVector[Double])): Option[OLSRegressionDense] = {

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

        return Some(new OLSRegressionDense(newXColNames, phenoName, newXValsAsMatrix, yVals))
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
    def rebuildModel(): OLSRegressionDense = {
      val xNames: Array[String] = collections.getAddedPrev.toArray
      val xVals: Array[Double] = xNames.flatMap(collections.addedPrevValues(_).toScalaVector())

      val yVals = broadcastPheno.value(phenoName)

      val numRows = yVals.length
      val numCols = xNames.length

      val xValsMatrix: DenseMatrix[Double] = new DenseMatrix(numRows, numCols, xVals)

      new OLSRegressionDense(xNames, phenoName, xValsMatrix, yVals)
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
  
  
  
}