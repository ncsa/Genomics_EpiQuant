package statistics

import breeze.linalg.pinv
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg.diag
import breeze.stats.distributions.StudentsT

/*
Summary of what we have and what we need:

################
We start with:
################

Y Vector
X Matrix (we add on a column of 1's and name it the intercept)

######################
Now we have/compute:
######################

N = length of Y
k = number of columns in X after adding the column of 1's

// Degrees of freedom
dof = N - k

// Behind the scenes (X^-1 * X^T) is only computed once, the value is stored
//   and just plugged in wherever it is used

coefficients = (X^-1 * X^T) * X^T * Y

fittedValues = X * (X^-1 * X^T) * X^T * Y

residuals = Y - fittedValues

// Also called SSR and SSE (very confusing when Googling this stuff)
RSS = sumOfSquared(residuals)

residualStandardError = sqrt(RSS/dof)

// This is a vector (we don't really use a for loop, we use a map)
standardErrors = 
   foreach i in diag(X^-1 * X^T) {
     return sqrt(i) * residualStandardError
   }

tStatistics = divide each coefficient by the corresponding standardError

pValues =
   foreach i in tStatistics {
     return 1 - StudentsT(dof).cdf( |t| ) * 2
   }

********************
What we still need
********************
SS
   There are different numbers reported for the mean and the error (also called the residuals I believe)
   The RSS we computed earlier is the sum of squared residuals
   
   SS_model = sumOf(y_hat - mean_y)^2
   SS_error = RSS

MS (just SS divided by the appropriate degrees of freedom?)

Also, when looking at a standard ANOVA table, I notice that the mean (also referred to as the "Factor") has m - 1 degrees of freedom,
  whereas the Error has n - m DoF, and they add up to n - 1 for the full model.
My question is, what does the m stand for? Is it the number of terms included in the model?

How do I calculate all of the following, using what I already have:

AIC
BIC
mBIC
model R^2

 */


class OLSRegression(val xColumnNames: Array[String],
                    val yColumnName: String, 
                    val Xs: scala.Vector[scala.Vector[Double]],
                    val Y: scala.Vector[Double]
                   ) extends java.io.Serializable {
  // Good summary of formula's used
  // http://www.stat.ucla.edu/~nchristo/introeconometrics/introecon_matrixform.pdf

  lazy private val sum = (i: DenseVector[Double]) => i.reduce((x,y) => x + y)
  lazy private val sumOfSquared = (i: DenseVector[Double]) => sum( i.map(math.pow(_, 2)))
  
  private val yAsBreezeVector = DenseVector(Y.toArray)
  
  // To estimate the intercept, a column of 1's is added to the matrix in the last position
  private val XsWith1sColumn = {
    val numRows = Y.size
    val numCols = Xs.transpose.apply(0).size
    val ones = List.fill(numRows)(1.0)
    new DenseMatrix(numRows, numCols + 1, Xs.flatten.toArray ++ ones)
  }  
  
  val N = Y.size
  private val k = XsWith1sColumn.cols

  // N - k (k is the number of estimations; this assumes that there is a 1's column for the intercept)
  val degreesOfFreedom = N - k

  private val transposedX = XsWith1sColumn.t
  
  /* Changed to use pseudoinverse, as Ellen said it will solve the SinglarMatrixException problem
   *   And all of the jUnit tests still passed, suggesting that the answers are still comparable to R's
   */
  private val inverseOfXtimesXt = pinv(transposedX * XsWith1sColumn)

  /** The estimates of the coefficients; the last entry is the estimate of the intercept */
  val coefficients = (inverseOfXtimesXt * transposedX * yAsBreezeVector).toArray
    
  /** 
   *  Predicted Y values, also known as Y_hat
   *  
   *  Y_hat = H Y
   *  where H is the hat matrix: H = X (X'X)^-1 X'
   */
  val fittedValues = XsWith1sColumn * inverseOfXtimesXt * transposedX * yAsBreezeVector
 
  /** Difference between the actual and predicted Y values */ 
  val residuals = yAsBreezeVector - fittedValues
  
  /**
   * Residual Sum of Squares
   * 
   *   Also called SSR (Sum of Squared Residuals) 
   *   Also called SSE (Sum of Squared Errors of prediction)
   */
  val RSS = sumOfSquared(residuals)
  
  /**
   * Residual Standard Error (known as RSE)
   */
  lazy val residualStandardError = math.sqrt( RSS / degreesOfFreedom )
  
  // http://avesbiodiv.mncn.csic.es/estadistica/ejemploaic.pdf
 // lazy val AIC = N * breeze.numerics.log(RSS/N) + 2*k
  
  // Need to filter out both cases where the Std.Err itself is NaN and when it is exactly zero
  
  // To prevent Std.Errors = 0 causing the T statistic to be NaN, we replace any zeroes with small, non-zero values
 // private val replaceNaN: Double => Double = x => if (x.isNaN()) Double.PositiveInfinity else x
  private val replaceZero: (Double => Double) = x => if (x == 0.0) 0.000001 else x
  
  /** Standard error for each coefficient; the final entry is for the intercept */
  val standardErrors = {
    val initial = diag(inverseOfXtimesXt).toArray.map(math.sqrt(_) * residualStandardError)
    val filtered = initial.map(replaceZero)
    filtered
  }
  
  /** T-statistic for each coefficient; the final entry is for the intercept */
  val tStatistics = for (i <- 0 until k) yield { coefficients(i) / standardErrors(i) }

  /** Performs Two-tailed test and gets a p-value from the T-statistic */
  private val tStatistic2pValue = (t: Double) => (1 - new StudentsT(degreesOfFreedom).cdf(math.abs(t))) * 2
  
  /** p-value for each coefficient; the final entry is for the intercept */
  val pValues = tStatistics.map(tStatistic2pValue(_)).toList
       
  /** Key is the name of the X variable, the value is the p-value associated with it */
  lazy val pValueMap = (xColumnNames :+ "intercept").zip(pValues).toMap
  
  lazy val lastXColumnsValues = {
    // Last column position (the very last position (k - 1) is the 1's column used to estimate the intercept)
    val pos = k - 2
    (0 until N).map(XsWith1sColumn(_, pos)).toVector
  }
  
  /** A summary of the regression stored as a single string ('\n' are included) */
  lazy val summaryString = {
    
    def standardizeLengths(arr: Array[String], rightPadding: Boolean = false) = {
      val maxLength = arr.map(_.length).max
      val padRight = (i: String) => i + " " * (maxLength - i.length)
      val padLeft = (j: String) => " " * (maxLength + 3 - j.length) + j
      if (rightPadding) arr.map(padRight) else arr.map(padLeft)
    }
    
    val names = "Name" +: xColumnNames :+ "(Intercept)"
          
    // The formatting below chops each double to show only a few decimal places
    val estimate = "Estimate" +: coefficients.map(x => f"$x%.6f".toString)
    val stdErr = "Std. Error" +: standardErrors.map(x => f"$x%.6f".toString)
    val tStat = "t value" +: tStatistics.toArray.map(x => f"$x%.3f".toString)
    val pValue = "Pr(>|t|)" +: pValues.toArray.map(x => f"$x%.6f".toString)
    
    val nameCol = standardizeLengths(names, rightPadding = true)
    val cols = nameCol +: Array(estimate, stdErr, tStat, pValue).map(standardizeLengths(_))
        
    val joinRow = (row: Array[String]) => (row :+ "\n").mkString
    val finalRows = cols.transpose.map(joinRow)
    val firstLine = "The Response variable: " + yColumnName + "\n\n"
    
    (firstLine +: finalRows).mkString("")
  }

  /** Prints a summary of the regression, in a format similar to R's summary */
  def printSummary {
    println(summaryString)
  }
}
