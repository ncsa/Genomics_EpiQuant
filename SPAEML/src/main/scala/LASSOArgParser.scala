import lasso._
import org.apache.spark.sql.SparkSession

case class LASSOConfig(
                        epiqInputFile: String = "",
                        pedInputFile: String = "",
                        mapInputFile: String = "",
                        phenotypeInputFile: String = "",
                        outputDirectoryPath: String = "",
                        isOnAws: Boolean = false,
                        s3BucketName: String = "",
                        sparkMaster: String = "local"
                      )

object LASSOArgParser {

  private val argParser = new scopt.OptionParser[LASSOConfig]("LASSO") {
    head("LASSO")

    note("Required Arguments\n------------------")

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

    opt[String]("epiq")
      .valueName("<file>")
      .action( (x, c) => c.copy(epiqInputFile = x) )
      .text("Path to the .epiq genotype input file")

    opt[String]("ped")
      .valueName("<file>")
      .action( (x, c) => c.copy(pedInputFile = x) )
      .text("Path to the .ped genotype input file")

    opt[String]("map")
      .valueName("<file>")
      .action( (x, c) => c.copy(mapInputFile = x) )
      .text("Path to the .map genotype input file")

    opt[Unit]("aws")
      .action( (_, c) => c.copy(isOnAws = true) )
      .text("Set this flag to run on AWS")

    opt[String]("bucket")
      .valueName("<url>")
      .action( (x, c) => c.copy(s3BucketName = x) )
      .text("Path to the S3 Bucket storing input and output files")

    opt[String]("master")
      .optional
      .valueName("<url>")
      .action( (x, c) => c.copy(sparkMaster = x) )
      .text("The master URL for Spark")

    checkConfig( c =>
      if (c.epiqInputFile.isEmpty && (c.pedInputFile.isEmpty || c.mapInputFile.isEmpty)) {
        failure("Need genotype input file: either specify one .epiq file or both .ped and .map files.")
      }
      else if (!c.epiqInputFile.isEmpty && !(c.pedInputFile.isEmpty && c.mapInputFile.isEmpty)) {
        failure("Conflicting genotype input files: either specify one .epiq file or both .ped and .map files.")
      }
      else {
        success
      }
    )

    checkConfig( c =>
      if (c.isOnAws && c.s3BucketName.isEmpty) failure("If the '--aws' flag is used, a S3 bucket path must be specified")
      else success
    )
  }

  def launch(args: Array[String]) {

    val parsed: Option[LASSOConfig] = argParser.parse(args, LASSOConfig())

    parsed match {

      // Received invalid or incomplete arguments
      case None => println("Invalid/incomplete arguments", new Error)

      // Received valid arguments
      case Some(_) => {

        val spark = if (parsed.get.isOnAws) {
          SparkSession
            .builder
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate
        } else {
          SparkSession
            .builder
            .master(parsed.get.sparkMaster)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate
        }

        spark.sparkContext.setLogLevel("ERROR")

        LASSO.performLASSO(
          spark,
          parsed.get.epiqInputFile,
          parsed.get.pedInputFile,
          parsed.get.mapInputFile,
          parsed.get.phenotypeInputFile,
          parsed.get.outputDirectoryPath,
          parsed.get.isOnAws,
          parsed.get.s3BucketName
        )
      }
    }
  }
}
