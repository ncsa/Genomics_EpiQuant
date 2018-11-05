import lasso.LASSO.train
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate

    train("/Users/stevenshang/genomics/SPAEML/src/test/resources/Genotypes/10.Subset.n=300.epiq",
      "/Users/stevenshang/genomics/SPAEML/src/test/resources/Phenotypes/Simulated.Data.1.Reps.Herit.0.92_n=300.epiq",
      spark.sparkContext)

//    def usage(): Unit = {
//      println("Usage: program [ConvertFormat | StepwiseModelSelection] [options]")
//    }
//
//    if (args.length == 0) usage
//    else {
//
//      val name = args(0)
//      name match {
//        case "ConvertFormat" => ConvertFormat.launch(args.drop(1))
//        case "StepwiseModelSelection" => StepwiseModelSelection.launch(args.drop(1))
//        case _ => usage
//      }
//    }
  }
  
}
