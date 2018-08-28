

object Main {
  
  def main(args: Array[String]) {
    if (args.length == 0) println("Enter ConvertFormat or StepwiseModelSelection")
    
    else {
      val program = args(0)
    
      if (program == "ConvertFormat") {
        // pass all arguments to ConvertFormat except for the program name itself
        ConvertFormat.launch(args.drop(1))
      }
      else if ( List("SEMS", "StepwiseModelSelection").contains(program) ) {
        // pass all arguments to StepwiseModelSelection except for the program name itself
        StepwiseModelSelection.launch(args.drop(1))
      }
      else {
        println("Did not recognize \"" + program + "\". Enter ConvertFormat or StepwiseModelSelection")
      }
    }
    
  }
  
}