package spaeml
  
import statistics._
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast._
import breeze.linalg.DenseVector
import org.apache.spark.storage.StorageLevel

/*
 * RANDOM NOTE
 *  (not 100 certain this is true; believed I saw evidence of this long ago. Need to reconsider)
 *
 *  Having a different threshold for the forward and backward steps can lead to oscillations
 *    where an X is included under high p-value, then skipped, then included again, ... and this goes on forever
 */

case class StepCollections(not_added: mutable.HashSet[String],
                           added_prev: mutable.HashSet[String] = mutable.HashSet(),
                           skipped: mutable.HashSet[String] = mutable.HashSet())

abstract class FileData(val sampleNames: Vector[String],
                        val dataPairs: Vector[(String, DenseVector[Double])]
                       ) {
  lazy val dataNames: Vector[String] = dataPairs.map(_._1)
}

trait SPAEML extends Serializable {
  
  /*
   * ABSTRACT FUNCTIONS
   */
  
  def readHDFSFile(filePath: String, spark: SparkContext): FileData
  def createPairwiseColumn(pair: (String, String),
                           broadMap: Broadcast[Map[String, DenseVector[Double]]]
                          ): (String, DenseVector[Double])
  
  /*
   * 1. Broadcast the original SNP table throughout the cluster
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
    *     9. For each phenotype, call the performSteps function (which implementation depends on whether Dense or
    *          Sparse vectors are used for the data. Currently, only SPAEML dense is implemented)
    *
    */
  def performSPAEML(spark: SparkSession,
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
    
    // Spread Vector of pairwise SNP_name combinations across cluster
    val pairwiseCombinations: Seq[(String, String)] = createPairwiseList(snpData.dataNames)
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

    // Broadcast Phenotype map where (phenotype_name -> [values])
    val phenoBroadcast = spark.sparkContext.broadcast(phenoData.dataPairs.toMap)
    val phenotypeNames = phenoData.dataNames

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
