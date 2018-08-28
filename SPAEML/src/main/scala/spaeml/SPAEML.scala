package spaeml
  
import statistics._
import scala.collection.mutable.HashSet
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast._
import breeze.linalg.DenseVector
import org.apache.spark.storage.StorageLevel

/*
 * Having a different threshold for the forward and backward steps can lead to oscillations
 *   where an X is included under high p-value, then skipped, then included again, ... and this goes on forever 
 */

case class StepCollections(not_added: HashSet[String],
                           added_prev: HashSet[String] = HashSet(),
                           skipped: HashSet[String] = HashSet())

abstract class FileData(val sampleNames: Vector[String],
                        val dataPairs: Vector[(String, DenseVector[Double])]
                       ) {
  lazy val dataNames: Vector[String] = dataPairs.map(_._1)
}

trait SPAEML extends Serializable {
  
  /*
   * ABSTRACT FUNCTIONS
   */
  
  def readHDFSFile(filePath: String, sark: SparkContext): FileData
  def createPairwiseColumn(pair: (String, String),
                           broadMap: Broadcast[Map[String, DenseVector[Double]]]
                          ): (String, DenseVector[Double])
  
  /*
   * 1. Broadcast the original table throughout the cluster
   * 2. Create and distribute a Vector of columnName pairs for the SNP table
   * 3. On each Executor, create the SNP pairs for the columnName pairs on that Executor
   * 4. Perform all of the regressions in a Map and Reduce style
   */  
  def performSteps(spark: SparkContext,
                   snpDataRDD: rdd.RDD[(String, DenseVector[Double])],
                   broadcastPhenotypes: Broadcast[Map[String, DenseVector[Double]]],
                   phenotypeName: String,
                   collections: StepCollections,
                   threshold: Double,
                   prev_best_model: OLSRegression, // Default value of null in subclasses,
                   iterations: Int // Default value of 1 in subclasses
                  ): OLSRegression

  /*
   * CONCRETE FUNCTIONS
   */
  
  protected def flattenArrayOfBreezeVectors(input: Array[breeze.linalg.Vector[Double]]): Array[Double] = {
    input.flatMap(breezeVect => breezeVect.toDenseVector.toScalaVector)
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
    val days = hours / 24.0
      
    val getTimeString = (s: String, d: Double) => "Calculation time (in " + s + "): " + d.toString + "\n"
    val timeString = getTimeString("seconds", seconds) + getTimeString("minutes", minutes) + getTimeString("hours", hours)
    return timeString
  }
  
  def performSEAMS(spark: SparkSession,
                  genotypeFile: String,
                  phenotypeFile: String,
                  outputFileDirectory: String,
                  threshold: Double,
                  serialization: Boolean
                 ) {
    
    val totalStartTime = System.nanoTime()
    
    val snpData = readHDFSFile(genotypeFile, spark.sparkContext)
    val phenoData = readHDFSFile(phenotypeFile, spark.sparkContext)

    if (phenoData.sampleNames != snpData.sampleNames) {
      throw new Error("Sample order did not match between the SNP and Phenotype input files")
    }

    // Broadcast original SNP_Phenotype map
    val broadSnpTable: Broadcast[Map[String, DenseVector[Double]]] =
      spark.sparkContext.broadcast(snpData.dataPairs.toMap)
    
    // Spread pairwise Vector across cluster
    val pairwiseCombinations = createPairwiseList(snpData.dataNames)

    val pairRDD = spark.sparkContext.parallelize(pairwiseCombinations)

    // Parallelize the original table into an RDD
    val singleSnpRDD = spark.sparkContext.parallelize(snpData.dataPairs)

    // Create the pairwise combinations across the cluster
    val pairedSnpRDD = pairRDD.map(createPairwiseColumn(_, broadSnpTable))

    val storageLevel = {
      if (serialization) StorageLevel.MEMORY_AND_DISK_SER 
      else StorageLevel.MEMORY_AND_DISK
    }

    val fullSnpRDD = (singleSnpRDD ++ pairedSnpRDD).persist(storageLevel)

    val phenoBroadcast = spark.sparkContext.broadcast(phenoData.dataPairs.toMap)
    val phenotypeNames = phenoData.dataNames

    // The :_* unpacks the contents of the array as input to the hash set
    val snpNames: HashSet[String] = HashSet(fullSnpRDD.keys.collect(): _*)

    // Create the initial set of collections (the class that keeps track of which SNPs are in and out of the model)
    //   All SNPs start in the not_added category
    val initialCollections = new StepCollections(not_added = snpNames)

    /*
     *  For each phenotype, build a model, println and save the results
     *
     *  TODO This loops through each phenotype in series and creates a model. Find a way to do this in parallel
     */
    for (phenotype <- phenotypeNames) {
      val startTime = System.nanoTime()
      
      val bestReg = performSteps(spark.sparkContext, fullSnpRDD, phenoBroadcast,
                                 phenotype, initialCollections, threshold, null, 1
                                )      
      val endTime = System.nanoTime()

      val timeString = constructTimeString(startTime, endTime)
      val summaryString = bestReg.summaryString
      val anovaSummaryString = bestReg.anovaTable.summaryString


      // Include both the standard R-like regression output and the ANOVA table style output
      val output = Array(timeString,
                         "Genotype File: " + genotypeFile,
                         "Phenotype File: " + phenotypeFile + "\n",
                         summaryString + "\n",
                         anovaSummaryString
                        )
      
      // Print
      output.foreach(println)

      // Save to file
      spark.sparkContext.parallelize(output, 1)
                        .saveAsTextFile(outputFileDirectory + "/" + phenotype + ".summary")
    }
    
    val totalEndTime = System.nanoTime()
    val totalTimeString = "\nTotal runtime (seconds): " + ((totalEndTime - totalStartTime) / 1e9).toString
    println(totalTimeString)
    
    spark.sparkContext.parallelize(List(totalTimeString), 1)
                        .saveAsTextFile(outputFileDirectory + "/total_time.log")
  }
  
}
