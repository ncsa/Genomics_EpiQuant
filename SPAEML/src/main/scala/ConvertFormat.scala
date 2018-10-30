import java.io.File
import converters._

case class Config(inputFiles: Seq[String] = Seq(),
                  outputFile: String =  ".",
                  inputType: String = "Custom",
                  deleteColumns: Seq[Int] = Seq(),
                  transpose: Boolean = false,
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

    opt[Boolean]('t', "transpose")
      .optional
      .valueName("{ default = false }")
      .text("Transpose the data")
      .action( (x, c) => c.copy(transpose = x) )

    opt[Seq[Int]]("deleteColumns")
      .optional
      .valueName("<Int>,<Int>,...")
      .text("Comma separated list of columns to delete; Count from 0")
      .action( (x, c) => c.copy(deleteColumns = x) )

    checkConfig( c =>
      if (c.inputFiles.size < 1) failure("You must specify at least one input file")
      else success
    )
  }

  def launch(args: Array[String]) = {

    val parsed = argParser.parse(args, Config())

    parsed match {

      // Handle a case where there is something wrong with the input arguments
      case None => System.err.println("\nError: Invalid/incomplete arguments")

      // If there is a valid set of arguments presented
      case Some(config) => {

        // FileParser is an abstract class
        parsed.get.inputType.toLowerCase match {
          case "custom" => {

            val inputFile = new File(parsed.get.inputFiles(0))
            val outputFile = new File(parsed.get.outputFile)

            new CustomFileParser(
              inputFile,
              parsed.get.delimiter,
              parsed.get.deleteColumns,
              parsed.get.transpose
            ).saveParsedFile(outputFile)
            println("Conversion successful: new file can be found at: " + outputFile.getAbsolutePath)
          }
          case "pedmap" => {

            if (parsed.get.inputFiles.size != 2) {
              throw new Error("Error: for PedMap parser, please specify a .ped file and a .map file")
            }

            val ped = parsed.get.inputFiles filter (x => x.takeRight(4) == ".ped")
            val map = parsed.get.inputFiles filter (x => x.takeRight(4) == ".map")

            if (ped.size != 1 || map.size != 1) {
              throw new Error("Error: for PedMap parser, please specify a .ped file and a .map file")
            }

            new PedMapParser(map(0), ped(0)).parseAndOutputToFile(parsed.get.outputFile)
            println("Conversion successful: new file can be found at: " + parsed.get.outputFile)
          }
          case _ => throw new Error("Error: Invalid Input Type")
        }
      }
    }
  }
}