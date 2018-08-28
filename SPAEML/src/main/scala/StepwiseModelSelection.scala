import java.io.File
import scopt._
import spaeml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

case class InputConfig(sparkMaster: String = "",
                       sparkLog: String = "INFO",
                       genotypeInput: String = ".",
                       phenotypeInput: String = ".",
                       output: String = ".",
                       threshold: Double = 0.05,
                       serialization: Boolean = false
                      )
                      
object StepwiseModelSelection {
  
  private val argParser = new scopt.OptionParser[InputConfig]("StepwiseModelSelection") {
    head("StepwiseModelSelection")

    note("Required Arguments\n------------------")
    
    opt[String]("spark-master")
      .required
      .valueName("<string>")
      .action( (x, c) => c.copy(sparkMaster = x) )
    
    opt[String]('G', "genotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(genotypeInput = x) )
      .text("Path to the genotype file")
    
    opt[String]('P', "phenotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(phenotypeInput = x) )
      .text("Path to the phenotype file")
      
    opt[String]('o', "output")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(output = x) )
      .text("Path to where the output file will be placed")
      
    note("\nOptional Arguments\n------------------")

    opt[Double]("threshold")
      .optional
      .valueName("<number>")
      .action( (x, c) => c.copy(threshold = x) )
      .text("The p-value threshold for the backward and forward steps: Defaults to 0.05")
      
    opt[String]("spark-log-level")
      .optional
      .valueName("WARN, INFO, DEBUG, etc.")
      .action( (x, c) => c.copy(sparkLog = x) )
      .text("Set sparks log verbosity: Defaults to INFO")
      
    opt[Boolean]("serialization")
      .optional
      .valueName("true or false")
      .text("Will data be serialized. Defaults to false; if true, need less memory, but runs slower")
      .action( (x, c) => c.copy(serialization = x) )

  }
  
  def launch(args: Array[String]) {
    
    val parsed = argParser.parse(args, InputConfig())
      
    parsed match {
      
      // Handle a case where there is something wrong with the input arguments
      case None => {
        System.err.println("\nError: Invalid/incomplete arguments")
      }
      
      // If there is a valid set of arguments presented
      case Some(config) => {
        
        val spark = SparkSession
                      .builder
                      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .appName("SEMS")
                      .master(parsed.get.sparkMaster)
                      .getOrCreate()
                     
        spark.sparkContext.setLogLevel(parsed.get.sparkLog)
        
        SEAMSDense.performSEAMS(spark,
                                parsed.get.genotypeInput,
                                parsed.get.phenotypeInput,
                                parsed.get.output,
                                parsed.get.threshold,
                                parsed.get.serialization
                               )
      }
    }  
  }
}
