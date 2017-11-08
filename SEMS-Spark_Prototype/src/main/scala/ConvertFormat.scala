import scopt._
import java.io.File
import converters._
import scala.collection.JavaConverters._

case class GeneralConfig(inputType: String = "Custom")

object ConvertFormat {
    
  private val fileTypes = List("plink", "Plink", "PLINK",
                               "custom", "Custom", "CUSTOM"
                              )
                              
  private val generalParser = new scopt.OptionParser[GeneralConfig]("ConvertFormat") {
    head("\nConvertFormat")
      
    opt[String]("inputType")
      .required
      .valueName("<String>")
      .action( (x, c) => c.copy(inputType = x) )
      .text("Input type: { Custom | Plink | HapMap }")
    
    note("\n")
  }
                                
  def launch(args: Array[String]) = { 
    
    val generalParsed = generalParser.parse(args, GeneralConfig())
    
    val config: GeneralConfig = {
      generalParsed match {
        // Handle a case where there is something wrong with the input arguments
        case None => throw new Error("\nError: Invalid/incomplete arguments")
        // If there is a valid set of arguments presented
        case Some(arg) => arg
      }
    }
    
    /* Because args should contain two arguments we want to remove when passing them to the file parser
     *   (the --inputType and the actual string that follows it), we will filter these out
     */
    val filteredArgs = {
      val indexOfFlag = args.indexOf("--inputType")
      // Replace the entry at the index and the one after it with nothing
      args.patch(indexOfFlag, Nil, 2) 
    }
      
    // FileParser is an abstract class
    val parser: FileParser = config.inputType match {
      case "custom" | "Custom" | "CUSTOM" => new CustomFileParser(filteredArgs)
      case _ => throw new Error("Error: Invalid Input Type")
    }
        
    parser.saveParsedFile()
    println("Conversion successful: new file can be found at: " + parser.getOutputPath())
  }
}