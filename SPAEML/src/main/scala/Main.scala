
object Main {

  def main(args: Array[String]) {

    def usage(): Unit = {
      println("Usage: program [ConvertFormat | StepwiseModelSelection | LASSO] [options]")
    }

    if (args.length == 0) usage
    else {

      val name = args(0)
      name match {
        case "ConvertFormat" => ConvertFormatArgParser.launch(args.drop(1))
        case "StepwiseModelSelection" => StepwiseModelSelectionArgParser.launch(args.drop(1))
        case "LASSO" => LASSOArgParser.launch(args.drop(1))
        case _ => usage
      }
    }
  }
  
}
