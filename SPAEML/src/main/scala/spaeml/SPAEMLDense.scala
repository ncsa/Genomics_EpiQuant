package spaeml

import statistics._
import scala.annotation.tailrec
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
                   broadcastPhenotypes: Broadcast[Map[String, DenseVector[Double]]],
                   phenotypeName: String,
                   collections: StepCollections,
                   threshold: Double,
                   prev_best_model: OLSRegression = null, // Initialized to null in super trait
                   iterations: Int = 1// Initialized to 1 in super trait
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

    /** Returns the name of the most recently added term */
    val getNewestTermsName = (reg: OLSRegression) => reg.xColumnNames.last

    /** */
    val getNewestTermsValues = (reg: OLSRegression) => reg.lastXColumnsValues()

    /**
     * Returns a OLSRegression object if the inputSnp is in the NotAdded category
     *
     *  Otherwise, an object of type None is returned (this SNP was not analyzed
     *    on this iteration)
     */
    def mapFunction(inputSnp: (String, DenseVector[Double]),
                    addedPrevBroadcast: Broadcast[Map[String, DenseVector[Double]]]
                   ): Option[OLSRegressionDense] = {
      
      // If the inputSnp is in the not_added category
      if ( collections.getNotAdded.contains(inputSnp._1) ) {
        val yVals = broadcastPhenotypes.value(phenotypeName)
        val xColNames = collections.getAddedPrev.toArray
        
        val numRows = yVals.length
        val numCols = xColNames.length
        
        val xVals: Array[Double] = xColNames.flatMap(addedPrevBroadcast.value(_))

        val newXColNames = xColNames :+ inputSnp._1
        
        val newXVals: Array[Double] = xVals ++ inputSnp._2
        val newXValsAsMatrix = new DenseMatrix(numRows, numCols + 1, newXVals)

        return Some(new OLSRegressionDense(newXColNames, phenotypeName, newXValsAsMatrix, yVals))
      } else {
        // Do not analyze this SNP
        return None
      }
    }

    def reduceFunction(inputRDD: rdd.RDD[Option[OLSRegression]]): OLSRegression = {
      val filtered = inputRDD.filter(x => x.isDefined).map(_.get)
      if (!filtered.isEmpty()) {
        filtered.reduce((x, y) => {
          if (getNewestTermsPValue(x) <= getNewestTermsPValue(y)) x else y
        })
      } else {
        /* warning: if this occurs the broadcast variable will not have been explicitly destroyed
         * Not certain spark automatically destroys it when it leaves scope, although that seems likely */
        // There are no more potential SNPs to be added
        throw new Exception("There are no more SNPs under consideration")
      }
    }

    /*
     * IMPLEMENTATION
     */

    /* First, create a Broadcast of the snp values that are already in the model so that their values
     *   will be available to all of the nodes (we know that they will all need copies of these)

     * Since a broadcast cannot be updated, these need to be recreated at the beginning of each iteration
     *   as the SNPs included in the models change
     */
    val addedPrevValMap = collections.getAddedPrev.toVector.map(name => {
                            Tuple2(name,
                                   /* Although lookup returns a Collection, in case the key is found on multiple
                                    *   partitions, we know that there is only one. We just grab it with head
                                    */
                                   snpDataRDD.lookup(name).head
                                  )
                          }).toMap
                          
    val addedPrevBroadcast = spark.broadcast(addedPrevValMap)

    /*
     *  Step 1: find the best regression for those SNPs still under consideration
     */

    // Map generates all of the regression outputs, and reduce finds the best one
    val mappedValues: rdd.RDD[Option[OLSRegression]] = snpDataRDD.map(mapFunction(_, addedPrevBroadcast))
    val bestRegression: OLSRegression = reduceFunction(mappedValues)

    bestRegression.printSummary()
    collections.getSkipped.foreach(x => print(x + ", "))

    // If the p-value of the newest term does not meet the threshold, return the prev_best_model
    if (getNewestTermsPValue(bestRegression) >= threshold) {
      if (prev_best_model != null) { return prev_best_model }
      else {
        addedPrevBroadcast.destroy()
        throw new Exception("No terms could be added to the model at a cutoff of " + threshold)
      }
    }
    else {
      val new_collections = collections.copy()

      // Now that the regressions for this round have been completed, return any entries in the skipped category to the
      //   not added category
      new_collections.getSkipped.foreach( x => new_collections.moveFromSkipped2NotAdded(x) )

      // Remove the newest term from the not_added category and put it in the added_prev category
      new_collections.moveFromNotAdded2AddedPrev(snpName = getNewestTermsName(bestRegression),
                                                 snpValues = getNewestTermsValues(bestRegression)
                                                 )

      /*
       * Step 2: Check to make sure none of the previously added terms are no longer significant
       * 				 If they are, remove them from consideration in the next round (put them in skipped) and
       *         take them out of the model
       */
      
      val namePValuePairs = bestRegression.xColumnNames.zip(bestRegression.pValues)
      
      namePValuePairs.foreach(pair => {
        if (pair._2 >= threshold) {
          // Remove this term from the prev_added collection, and move it to the skipped category
          new_collections.moveFromAddedPrev2Skipped(pair._1)
        }
      })
        
      // Add the name to the entriesSkippedThisRound variable so that at the end, this can be removed
          // from the BestRegression that is passed to the next iteration
      val entriesSkippedThisRound = namePValuePairs.filter(_._2 >= threshold).map(_._1)
      
      if (new_collections.getNotAdded.isEmpty) {
        /*
         * No more terms that could be added. Return the current best model, unless there are entries
         * in the skipped category.
         * 
         * If this is the case, perform one last regression with the current add_prev collection in 
         * the model. 
         * 
         * If something is in the skipped category at this point it was added during this iteration.
         * and the "bestRegression" variable will still have that term included.
         */
        if (new_collections.getSkipped.isEmpty) {return bestRegression}
        else {
          
          val newestXColName = bestRegression.xColumnNames.last
          val otherXColNames = new_collections.getAddedPrev.filterNot(_ == newestXColName).toArray

          // New x values: look up the previously added from the broadcast table, then include the values of
          //   the latest term to be added
          // val xVals = otherXColNames.map(addedPrevBroadcast.value(_)).toVector :+ bestRegression.lastXColumnsValues
          val xVals: Array[BreezeVector[Double]] =
            otherXColNames.map(addedPrevBroadcast.value(_)) :+ bestRegression.lastXColumnsValues

          val yVals = broadcastPhenotypes.value(phenotypeName)

          // We must convert to Scala Vector because BreezeVectors are not able to be flattened)
          val flattenedXValues = flattenArrayOfBreezeVectors(xVals)
          val xColNames = newestXColName +: otherXColNames
          val numRows = yVals.length
          val numCols = xColNames.length
          val xValsMatrix: DenseMatrix[Double] = new DenseMatrix(numRows, numCols, flattenedXValues)
          
          addedPrevBroadcast.destroy()

          return new OLSRegressionDense(xColNames, phenotypeName, xValsMatrix, yVals)
        }
      } else {
        
        /*
         * If any entries were skipped this round, recompute the regression without these terms,
         *  and then include the new best regression in the next iteration
         */
        val newBestRegression = {
          
          if (entriesSkippedThisRound.length > 0) {
            
            val newestXColName = bestRegression.xColumnNames.last
            val otherXColNames = new_collections.getAddedPrev.filterNot(_ == newestXColName).toArray
          
            val nameInSkipped: (String => Boolean) = entriesSkippedThisRound.contains(_)
          
            val significantXColNames = otherXColNames.filterNot(nameInSkipped(_))
            // New x values: look up the previously added from the broadcast table, then include the values of
            //   the latest term to be added
            val xVals = significantXColNames.map(addedPrevBroadcast.value(_)) :+ bestRegression.lastXColumnsValues

            val yVals = broadcastPhenotypes.value(phenotypeName)

            val flattenedXValues = flattenArrayOfBreezeVectors(xVals)
            val xColNames = newestXColName +: significantXColNames
            val numRows = yVals.length
            val numCols = xColNames.length
            val xValsMatrix: DenseMatrix[Double] = new DenseMatrix(numRows, numCols, flattenedXValues)
            
            new OLSRegressionDense(xColNames, phenotypeName, xValsMatrix, yVals)
          }
          else {
            bestRegression
          }
        }
        
        addedPrevBroadcast.destroy() 
        
        performSteps(spark, snpDataRDD, broadcastPhenotypes, phenotypeName,
                     new_collections, threshold, newBestRegression, iterations + 1
                    )
      }
    }
  }
  
  
  
}