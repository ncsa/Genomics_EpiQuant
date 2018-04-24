import statistics._
import breeze.linalg.DenseMatrix
import breeze.linalg.DenseVector
import breeze.linalg.CSCMatrix
import breeze.linalg.pinv

object Test extends App {
  
/*
x1 <- c(1.0, 2, 5, 6, 7, 8, 9, 8, 8, 4, 5, 7, 9)
x2 <- c(4.0, 2, 3, 7, 5, 9, 4, 4, 5, 6, 9, 10, 12)
x3 <- c(3.0, 4, 2, 6, 8, 8, 8, 8, 3, 4, 8, 1, 9)

y <- c(3.0, 5.5, 6.0, 9.0, 10, 7, 8, 12, 13, 14, 16, 15, 15)
model <- lm(y~x1+x2+x3)
summary(model)
anova(model)
AIC(model)

x <- c(1.0,2,3,5,7)
y <- c(1.0,2,3,4,5)
model <- lm(y~x)
summary(model)
anova(model)
AIC(model)

*/
 
  //val x_names = Array("x1", "x2", "x3")
  val x_names = Array("x1")//, "x2", "x3")
  val y_name = "y"
  
  val x1 = Vector(1.0, 2, 5, 6, 7, 8, 9, 8, 8, 4, 5, 7, 9)
  val x2 = Vector(4.0, 2, 3, 7, 5, 9, 4, 4, 5, 6, 9, 10, 12)
  val x3 = Vector(3.0, 4, 2, 6, 8, 8, 8, 8, 3, 4, 8, 1, 9)
  
  //val flattenedXValues: Array[Double] = Array(x1, x2, x3).flatten
  val flattenedXValues: Array[Double] = x1.toArray

  //val xs: DenseMatrix[Double] = new DenseMatrix(x1.length, 3, flattenedXValues )
  val xs: DenseMatrix[Double] = new DenseMatrix(x1.length, 1, flattenedXValues )

  val y = DenseVector(3.0, 5.5, 6.0, 9.0, 10, 7, 8, 12, 13, 14, 16, 15, 15)
  
  val model = new OLSRegressionDense(x_names, y_name, xs, y)
  //val model = new OLSRegression(Array("x1"), y_name, Vector(x1), y)
  val printFeature = (s: String, f: Double) => println(s + ": " + f"$f%.4f".toString)
  
  //println(model.summaryString)

  model.anovaTable.printTable()

}