import statistics._
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import breeze.linalg.CSCMatrix
import breeze.linalg.pinv

object Test extends App {
  
/*
x1 <- c(1.0, 2.0, 5.0, 6.0, 7, 8)
x2 <- c(4.0, 2.0, 3.0, 7.0, 5, 9)
x3 <- c(3.0,   4,   2,   6, 8, 8)

y <- c(3.0, 5.5, 6.0, 9.0, 10, 7)

model <- lm(y~x1+x2+x3)
summary(model)
anova(model)
AIC(model)
*/
 
  val x_names = Array("x1", "x2", "x3")
  val y_name = "y"
  
  val x1 = Vector(1.0, 2.0, 5.0, 6.0, 7, 8)
  val x2 = Vector(4.0, 2.0, 3.0, 7.0, 5, 9)
  val x3 = Vector(3.0, 4, 2, 6, 8, 8)
  
  val flattenedXValues: Array[Double] = Array(x1, x2, x3).flatten
  // val flattenedXValues: Array[Double] = Vector(1.0, 2.0, 5.0, 6.0, 7, 8, 4.0, 2.0, 3.0, 7.0, 5, 9, 3.0, 4, 2, 6, 8, 8).toArray

  val xs: DenseMatrix[Double] = new DenseMatrix(x1.length, 3, flattenedXValues )
  
  val y = DenseVector(3.0, 5.5, 6.0, 9.0, 10, 7)
  
  
  val model = new OLSRegressionDense(x_names, y_name, xs, y)
  //val model = new OLSRegression(Array("x1"), y_name, Vector(x1), y)
  val printFeature = (s: String, f: Double) => println(s + ": " + f"$f%.4f".toString)
  
  println(model.summaryString)
  
  printFeature("R-squared", model.R_squared)
  printFeature("Adjusted R-squared", model.adjusted_R_squared)
  printFeature("Log-Likelihood", model.log_likelihood)
  printFeature("AIC", model.AIC)
  printFeature("BIC", model.BIC)
  printFeature("Df_model", model.DoF_model)
  printFeature("Df_error", model.DoF_error)
  printFeature("total_SS_model", model.total_SS_model)
  printFeature("SS_error (SSR)", model.RSS)
  printFeature("total_MS_model", model.total_MS_model)
  printFeature("MS_error", model.MS_error)
  printFeature("F_statistic_model", model.F_statistic_model)
  printFeature("p-value model", model.p_value_model)
  
  val builder = new CSCMatrix.Builder[Double](rows=10, cols=10)
  builder.add(1,1,2.5)
    
  val builder2 = new CSCMatrix.Builder[Double](rows=10, cols=10)
  builder.add(1,6,22.5)
  
  val A = builder.result
  val B = builder2.result
    
  val C = A * B.t
  val D = pinv(C.toDenseMatrix)
}