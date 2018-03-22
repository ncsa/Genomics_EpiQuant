package statistics

import breeze.linalg.pinv
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg.diag

class OLSRegressionDense(xColumnNames: Array[String],
                         yColumnName: String, 
                         Xs: DenseMatrix[Double],
                         Y: DenseVector[Double]
                        ) 
    extends OLSRegression(xColumnNames, yColumnName, Xs, Y) {
  
  // To estimate the intercept, a column of 1's is added to the matrix in the last position
  lazy val XMatrixWith1sColumn: DenseMatrix[Double] = {
    val ones: Array[Double] = Array.fill(Xs.rows)(1.0)
    val flattenedValues: Array[Double] = Xs.toArray ++ ones
    new DenseMatrix(Xs.rows, Xs.cols + 1, flattenedValues)
  }
  
  lazy val transposedX = XMatrixWith1sColumn.t
  
  /* Uses pseudoinverse, as it will solve the SinglarMatrixException problem
   *   And all of the jUnit tests still passed, suggesting that the answers
   *   are still comparable to R's
   */
  lazy val inverseOfXtimesXt = pinv(transposedX * XMatrixWith1sColumn)

  /** Standard error for each coefficient; the final entry is for the intercept */
  lazy val standardErrors = {
    val initial = diag(inverseOfXtimesXt).map(math.sqrt(_) * residualStandardError)
    val filtered = initial.map(replaceZero)
    filtered
  }
  
  def lastXColumnsValues(): DenseVector[Double] = {
    // Last column position (the very last position (k - 1) is the 1's column used to estimate the intercept)
    val pos = k - 2
    val a = (0 until N).map( XMatrixWith1sColumn(_, pos))
    DenseVector( 
                // The ':_*' unpacks the values in the collection and passes them each to the DenseVector constructor
                (0 until N).map( XMatrixWith1sColumn(_, pos) ) :_* 
               )
  }

}