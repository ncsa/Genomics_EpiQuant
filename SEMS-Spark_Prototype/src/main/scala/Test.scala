import statistics._

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
  
  val xs = Vector(x1, x2, x3)
  
  val y = Vector(3.0, 5.5, 6.0, 9.0, 10, 7)
  
  
  val model = new OLSRegression(x_names, y_name, xs, y)
  
  val printFeature = (s: String, f: Double) => println(s + ": " + f"$f%.4f".toString)
  
  println(model.summaryString)
  
  printFeature("R-squared", model.R_squared)
  printFeature("Adjusted R-squared", model.adjusted_R_squared)
  printFeature("Log-Likelihood", model.log_likelihood)
  printFeature("AIC", model.AIC)
  printFeature("BIC", model.BIC)
  printFeature("Df_model", model.DoF_model)
  printFeature("Df_error", model.DoF_error)
  printFeature("SS_model", model.SS_model)
  printFeature("SS_error (SSR)", model.RSS)
  printFeature("MS_model", model.MS_model)
  printFeature("MS_error", model.MS_error)
}