
object Main {

  def main(args: Array[String]) {

    def usage(): Unit = {
      println("Usage: program [ConvertFormat | StepwiseModelSelection] [options]")
    }

    if (args.length == 0) usage
    else {

      val name = args(0)
      name match {
        case "ConvertFormat" => ConvertFormat.launch(args.drop(1))
        case "StepwiseModelSelection" => StepwiseModelSelection.launch(args.drop(1))
        case _ => usage
      }
    }
  }
  
}
