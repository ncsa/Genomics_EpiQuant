package statistics

import breeze.linalg.{DenseMatrix, DenseVector, diag, pinv}
import breeze.stats.distributions.StudentsT
import breeze.stats.distributions.FDistribution
import breeze.numerics.log
import loggers.EpiQuantLogger

class OLSRegression(val xColumnNames: Array[String],
                    val yColumnName: String,
                    val Xs: DenseMatrix[Double],
                    val Y: breeze.linalg.DenseVector[Double]
                   ) extends java.io.Serializable {

  // Good summary of formula's used
  // http://www.stat.ucla.edu/~nchristo/introeconometrics/introecon_matrixform.pdf
    
  /*
   * Also called a design matrix
   * To estimate the intercept, a column of 1's is added to the matrix in the last position
   */

  def sum(v: breeze.linalg.Vector[Double]): Double = v.reduce((x,y) => x + y)
  def sumOfSquared(i: breeze.linalg.Vector[Double]): Double = sum( i.map(math.pow(_, 2)))
  
  // To prevent Std.Errors = 0 causing the T statistic to be NaN, we replace any zeroes with small, non-zero values
  protected def replaceZero(x: Double): Double = if (x == 0.0) 0.000001 else x

  // To estimate the intercept, a column of 1's is added to the matrix in the last position
  lazy val XMatrixWith1sColumn: DenseMatrix[Double] = {
    val ones: Array[Double] = Array.fill(Xs.rows)(1.0)
    val flattenedValues: Array[Double] = Xs.toArray ++ ones
    new DenseMatrix(Xs.rows, Xs.cols + 1, flattenedValues)
  }

  val meanY: Double = sum(Y) / Y.length
  lazy val N: Int = Y.size
  
  // Also referred to as p (this is the number of Beta_i parameters; includes the intercept)
  protected val k: Int =  XMatrixWith1sColumn.cols

  // N - k (k is the number of estimations; this assumes that there is a 1's column for the intercept)
  protected val degreesOfFreedom: Int = N - k
  
  /** k - 1 */
  val DoF_model: Int = k - 1
    
  /** n - k */
  val DoF_error: Int = N - k

  val transposedX: DenseMatrix[Double] = XMatrixWith1sColumn.t

  /* Uses pseudoinverse, as it will solve the SinglarMatrixException problem
   *   And all of the jUnit tests still passed, suggesting that the answers
   *   are still comparable to R's
   */
  val inverseOfXtimesXt = pinv(transposedX * XMatrixWith1sColumn)

  /** The estimates of the coefficients; the last entry is the estimate of the intercept */
  val coefficients: breeze.linalg.Vector[Double] = inverseOfXtimesXt * transposedX * Y
    
  /** 
   *  Predicted Y values, also known as Y_hat
   *  
   *  Y_hat = H Y
   *  where H is the hat matrix: H = X (X'X)^-1 X'
   */
  val fittedValues: breeze.linalg.Vector[Double] = XMatrixWith1sColumn * inverseOfXtimesXt * transposedX * Y
 
  /** Difference between the actual and predicted Y values */ 
  val residuals: breeze.linalg.Vector[Double] = Y - fittedValues
  
  /**
   * Residual Sum of Squares
   * 
   *   Also called SSR (Sum of Squared Residuals) 
   *   Also called SSE (Sum of Squared Errors of prediction)
   *   Also called SS_error
   */
  val RSS: Double = sumOfSquared(residuals)
  
  /**
   * SS Model
    * *
    *  Also called SS Reg (Sum of Squares due to regression)
   * 
   * SS_model = sumOf(y_hat - mean_y)^2
   */  
  lazy val SS_model: Double = sumOfSquared(fittedValues - meanY)
  lazy val MS_model: Double = SS_model / DoF_model
  
  lazy val MS_error: Double = RSS / DoF_error
  
  /**
   * log-likelihood
   * 
   *   // The equation from this source was not right
   *   http://www.stat.wisc.edu/courses/st333-larget/aic.pdf
   *   n + n * log(2*pi) + n * log(RSS/n)
   * 
   * Got the following equation to give results equivalent to R (from looking at R source)
   * https://github.com/wch/r-source/blob/af7f52f70101960861e5d995d3a4bec010bc89e6/src/library/stats/R/logLik.R
   * But do not understand why it works (Why the -N/2 multiplier?)
   * 
   */

  lazy val log_likelihood: Double = -N/2 * (log(2 * math.Pi) + 1 + log(RSS/N)) // CORRECT, SIMPLIFIED EQUATION

  /**
   * AIC
   * 
   * https://dl.sciencesocieties.org/publications/aj/articles/107/2/786
   * 
   * p_subcript_i = k + 1
   */
  lazy val AIC: Double = -2 * log_likelihood + 2 * (k + 1)
  
  /**
   * BIC
   * 
   * https://dl.sciencesocieties.org/publications/aj/articles/107/2/786
   * 
   * p_subcript_i = k + 1
   */
  lazy val BIC: Double = -2 * log_likelihood + log(N) * (k + 1)
  
  /** 
   *  mBIC (invented in 2008)
   *  
   *  Paper:
   *    http://onlinelibrary.wiley.com/doi/10.1111/j.1541-0420.2008.00989.x/full
   */
  
  lazy val SST: Double = RSS + SS_model
  
  lazy val R_squared: Double = 1 - RSS / SST
  lazy val adjusted_R_squared: Double = 1 - ( RSS / (N - k)) / (SST / (N - 1) )
  
  /**
   * F statistic for the model = MS_model / MS_error
   */
  lazy val F_statistic_model: Double = MS_model / MS_error

  /**
   * The p-value of the model
   */
  lazy val p_value_model: Double = 1 - new FDistribution(DoF_model, DoF_error).cdf(F_statistic_model)
  
  /**
   * Residual Standard Error (known as RSE)
   */
  lazy val residualStandardError: Double = math.sqrt( RSS / degreesOfFreedom )

  /** Standard error for each coefficient; the final entry is for the intercept */
  lazy val standardErrors: DenseVector[Double] = {
    val initial = diag(inverseOfXtimesXt).map(math.sqrt(_) * residualStandardError)
    val filtered = initial.map(replaceZero)
    filtered
  }

  // http://avesbiodiv.mncn.csic.es/estadistica/ejemploaic.pdf
 // lazy val AIC = N * breeze.numerics.log(RSS/N) + 2*k
  
  /** T-statistic for each coefficient; the final entry is for the intercept */
  lazy val tStatistics: IndexedSeq[Double] = for (i <- 0 until k) yield { coefficients(i) / standardErrors(i) }

  /** Performs Two-tailed test and gets a p-value from the T-statistic */
  lazy private val tStatistic2pValue = (t: Double) => (1 - new StudentsT(degreesOfFreedom).cdf(math.abs(t))) * 2
  
  /** p-value for each coefficient; the final entry is for the intercept */
  lazy val pValues: List[Double] = tStatistics.map(tStatistic2pValue(_)).toList
  
  /** Key is the name of the X variable, the value is the p-value associated with it */
  lazy val pValueMap: Map[String, Double] = (xColumnNames :+ "intercept").zip(pValues).toMap

  /** Array of lines representing a summary of the model */
  lazy val summaryStrings: Array[String] = {
    
    def standardizeLengths(arr: Array[String], rightPadding: Boolean = false) = {
      val maxLength = arr.map(_.length).max
      val padRight = (i: String) => i + " " * (maxLength - i.length)
      val padLeft = (j: String) => " " * (maxLength + 3 - j.length) + j
      if (rightPadding) arr.map(padRight) else arr.map(padLeft)
    }
    
    val names = "Name" +: xColumnNames :+ "(Intercept)"
          
    // The formatting below chops each double to show only a few decimal places
    val estimate = "Estimate" +: coefficients.toArray.map(x => f"$x%.6f".toString)
    val stdErr = "Std. Error" +: standardErrors.toArray.map(x => f"$x%.6f".toString)
    val tStat = "t value" +: tStatistics.toArray.map(x => f"$x%.3f".toString)
    val pValue = "Pr(>|t|)" +: pValues.toArray.map(x => f"$x%.6f".toString)
    
    val nameCol = standardizeLengths(names, rightPadding = true)
    val cols = nameCol +: Array(estimate, stdErr, tStat, pValue).map(standardizeLengths(_))
        
    val joinRow = (row: Array[String]) => (row).mkString
    val finalRows = cols.transpose.map(joinRow)
    val firstLine = "The Response variable: " + yColumnName
    
    firstLine +: finalRows
  }

  def lastXColumnsValues(): DenseVector[Double] = {
    // Last column position (the very last position (k - 1) is the 1's column used to estimate the intercept)
    val pos = k - 2
    DenseVector(
      // The ':_*' unpacks the values in the collection and passes them each to the DenseVector constructor
      (0 until N).map( XMatrixWith1sColumn(_, pos) ) :_*
    )
  }

  /*
 This must be lazy, because the AVOVATable class itself creates an instance of this object, but without looking
   at its anova results. If this weren't lazy, it will create an loop of object creation,
   where OLSRegression -> ANOVATable -> OLSRegression -> ANOVATable -> ...
 */
  lazy val anovaTable: ANOVATable = new ANOVATable(this)

  /** Logs a summary of the regression, in a format similar to R's summary */
  def logSummary(): Unit = {
    EpiQuantLogger.info(summaryStrings.mkString("\n\t"))
  }
}


