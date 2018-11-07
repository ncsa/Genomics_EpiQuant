import java.io.File
import converters._
import loggers.EpiQuantLogger

case class Config(inputFiles: Seq[String] = Seq(),
                  outputFile: String = ".",
                  inputType: String = "custom",
                  deleteColumns: Seq[Int] = Seq(),
                  columnsAreVariants: Boolean = false,
                  delimiter: String = "\t"
                 )

object ConvertFormat {

  private val fileTypes = List("pedmap", "custom")

  private val argParser = new scopt.OptionParser[Config]("ConvertFormat") {
    head("\nConvertFormat")

    note("Required Arguments\n------------------")

    opt[Seq[String]]('i', "inputs")
      .required
      .valueName("<String>,<String>")
      .action( (x, c) => c.copy(inputFiles = x) )
      .text("Paths of the files to convert")

    opt[String]('o', "output")
      .required
      .valueName("<String>")
      .action( (x, c) => c.copy(outputFile = x) )
      .text("Path where the output file will be placed")

    opt[String]("inputType")
      .required
      .valueName("<String>")
      .action( (x, c) => c.copy(inputType = x) )
      .text("The format of the input file { pedmap | custom }")
      .validate {x =>
        if (fileTypes.contains(x.toLowerCase)) success
        else failure("File type must be either pedmap or custom")
      }

    note("\nOptional Arguments\n------------------")

    opt[String]('d', "delimiter")
      .optional
      .valueName("{ default = <tab> }")
      .text("Set what delimiter to use")
      // Allows user to enter \t for a tab, although technically the program needs \\t
      .action{
      (x, c) => {
        if (x == "t") c.copy(delimiter = "\\t")
        else c.copy(delimiter = x )
      }
    }

    opt[Boolean]("columnsAreVariants")
      .optional
      .valueName("{ default = false }")
      .text("(custom-only) Variants are stored in columns")
      .action( (x, c) => c.copy(columnsAreVariants = x) )

    opt[Seq[Int]]("deleteColumns")
      .optional
      .valueName("<Int>,<Int>,...")
      .text("(custom-only) Comma separated list of columns to delete; Count from 0")
      .action( (x, c) => c.copy(deleteColumns = x) )

    checkConfig( c =>
      if (c.inputFiles.size < 1) failure("You must specify at least one input file")
      else success
    )
  }

  def launch(args: Array[String]): Unit = {

    val parsed = argParser.parse(args, Config())

    parsed match {

      // Handle a case where there is something wrong with the input arguments
      case None => EpiQuantLogger.error("Invalid/incomplete arguments", new Error)

      // If there is a valid set of arguments presented
      case Some(config) => {

        // FileParser is an abstract class
        parsed.get.inputType.toLowerCase match {
          case "custom" => {

            new CustomFileParser(parsed.get.inputFiles(0),
                                 parsed.get.delimiter,
                                 parsed.get.deleteColumns,
                                 parsed.get.columnsAreVariants
                                ).writeEpiqFile(parsed.get.outputFile)

            EpiQuantLogger.info(
              "Conversion successful: new file can be found at: " + new File(parsed.get.outputFile).getAbsolutePath
            )
          }
          case "pedmap" => {
            val pedmapErrorMessage = "For PedMap parser, please specify a .ped file and a .map file"

            if (parsed.get.inputFiles.size != 2) EpiQuantLogger.error(pedmapErrorMessage ,new Error)

            val ped = parsed.get.inputFiles filter (x => x.takeRight(4) == ".ped")
            val map = parsed.get.inputFiles filter (x => x.takeRight(4) == ".map")

            if (ped.size != 1 || map.size != 1) EpiQuantLogger.error(pedmapErrorMessage, new Error)

            new PedMapParser(map(0), ped(0)).writeEpiqFile(parsed.get.outputFile)
            EpiQuantLogger.info(
              "Conversion successful: new file can be found at: " +  new File(parsed.get.outputFile).getAbsolutePath
            )
          }
          case other => EpiQuantLogger.error("Invalid input type: " + other, new Error)
        }
      }
    }
  }
}