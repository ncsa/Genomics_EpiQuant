package converters

import java.io.File

case class CustomConfig(input: File = new File("."),
                        output: File = new File("."),
                        inputType: String = "Custom",
                        deleteColumns: Seq[Int] = Seq(),
                        transpose: Boolean = false,
                        delimiter: String = "\\t"
                       )
                       
object CustomFileParser {
  
  val customParser = new scopt.OptionParser[CustomConfig]("Custom") {
    head("")

    note("Required Arguments\n------------------")

    opt[File]('i', "input")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(input = x) )
      .text("Path of the file to convert")
        
    opt[File]('o', "output")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(output = x) )
      .text("Path where the output file will be placed")
      
    note("\nOptional Arguments\n------------------")

    opt[String]('d', "delimiter")
      .optional
      .valueName("{ default = <tab> }")
      .text("Set what delimiter to use")
      // Allows user to enter \t for a tab, although technically the program needs \\t
      .action( (x, c) => { if (x == "t") c.copy(delimiter = "\\t") else c.copy(delimiter = x ) } )
      
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
      
    note("\n")
  }
  
  def showParserUsage() = CustomFileParser.customParser.showUsage()
}

class CustomFileParser(args: Array[String]) extends FileParser {
  
  private val customParsed = CustomFileParser.customParser.parse(args, CustomConfig())
  
  private val config: CustomConfig = {
    customParsed match {
      case None => throw new Error("\nError: Invalid/incomplete arguments")
      case Some(arg) => arg
    }
  }
  
  def saveParsedFile() = {
    val inputTable = this.readFile(config.input, config.delimiter)
  
    val convertedTable = {
      val filtered = inputTable.deleteColumns(config.deleteColumns: _*)
      if (config.transpose) filtered.transpose else filtered
    }
    convertedTable.replaceTopLeftEntry.saveTableAsTSV(config.output)
  }
   
  def getOutputPath(): String = config.output.getAbsolutePath.toString
  
}