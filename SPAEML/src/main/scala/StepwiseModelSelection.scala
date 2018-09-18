import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import spaeml._
import org.apache.spark.sql.SparkSession

case class InputConfig(
                        genotypeInputFile: String = "",
                        phenotypeInputFile: String = "",
                        outputDirectoryPath: String = "",
                        aws: Boolean = false,
                        s3BucketPath: String = "",
                        threshold: Double = 0.05,
                        serialize: Boolean = false,
                        sparkMaster: String = "YARN"
                      )
                      
object StepwiseModelSelection {

  def clearS3OutputDirectory(spark: SparkSession, s3BucketPath: String, outputDirectory: String): Unit = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(URI.create(s3BucketPath), conf)
    val outDirPath = new Path(s3BucketPath + outputDirectory)

    if (fs.exists(outDirPath)) {
      fs.delete(outDirPath, true)
      println("Deleting output directory: " + outDirPath)
    }
  }

  def clearLocalOutputDirectory(spark: SparkSession, outputDirectory: String): Unit = {

    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val outDirPath = new Path(outputDirectory)

    if (fs.exists(outDirPath)) {
      fs.delete(outDirPath, true)
      println("Deleting output directory: " + outDirPath)
    }
  }

  private val argParser = new scopt.OptionParser[InputConfig]("StepwiseModelSelection") {
    head("StepwiseModelSelection")

    note("Required Arguments\n------------------")

    opt[String]('G', "genotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(genotypeInputFile = x) )
      .text("Path to the genotype input file")
    
    opt[String]('P', "phenotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(phenotypeInputFile = x) )
      .text("Path to the phenotype input file")
      
    opt[String]('o', "output")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(outputDirectoryPath = x) )
      .text("Path to the output directory")
      
    note("\nOptional Arguments\n------------------")

    opt[Unit]("aws")
      .action( (_, c) => c.copy(aws = true) )
      .text("Set this flag to run on AWS")

    opt[Unit]("serialize")
      .action( (_, c) => c.copy(serialize = true) )
      .text("Set this flag to serialize data, which is space-efficient but CPU-bound")

    opt[String]("bucket")
        .valueName("<url>")
        .action( (x, c) => c.copy(s3BucketPath = x) )
        .text("Path to the S3 Bucket storing input and output files")

    opt[Double]("threshold")
      .optional
      .valueName("<number>")
      .action( (x, c) => c.copy(threshold = x) )
      .text("The p-value threshold for the backward and forward steps (default=0.05)")

    opt[String]("master")
        .valueName("<url>")
        .action( (x, c) => c.copy(sparkMaster = x) )
        .text("The master URL for Spark")

    checkConfig( c =>
      if (c.aws && c.s3BucketPath.isEmpty) failure("If the '--aws' flag is used, a S3 bucket path must be specified")
      else if (!c.aws && c.sparkMaster == "YARN") failure("If the '--aws' flag is not used, a spark master must be specified")
      else success
    )
  }
  
  def launch(args: Array[String]) {
    
    val parsed = argParser.parse(args, InputConfig())
    
    parsed match {
      
      // Handle a case where there is something wrong with the input arguments
      case None => {
        System.err.println("\nError: Invalid/incomplete arguments")
      }
      
      // If there is a valid set of arguments presented
      case Some(_) => {

        if (parsed.get.aws) {

          val spark = SparkSession
            .builder
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .appName("SPAEML")
            .getOrCreate()

          spark.sparkContext.setLogLevel("INFO")

          clearS3OutputDirectory(spark, parsed.get.s3BucketPath, parsed.get.outputDirectoryPath)

          SPAEMLDense.performSPAEML(
            spark,
            parsed.get.s3BucketPath + parsed.get.genotypeInputFile,
            parsed.get.s3BucketPath + parsed.get.phenotypeInputFile,
            parsed.get.s3BucketPath + parsed.get.outputDirectoryPath,
            parsed.get.threshold,
            parsed.get.serialize
          )
        } else {

          val spark = SparkSession
            .builder
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .appName("SPAEML")
            .master(parsed.get.sparkMaster)
            .getOrCreate()

          spark.sparkContext.setLogLevel("INFO")

          clearLocalOutputDirectory(spark, parsed.get.outputDirectoryPath)
          
          SPAEMLDense.performSPAEML(
            spark,
            parsed.get.genotypeInputFile,
            parsed.get.phenotypeInputFile,
            parsed.get.outputDirectoryPath,
            parsed.get.threshold,
            parsed.get.serialize
          )
        }
      }
    }  
  }
}
