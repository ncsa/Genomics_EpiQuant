package statistics

import breeze.linalg.pinv
import breeze.linalg.SparseVector
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg.CSCMatrix
import breeze.linalg.diag
/*
class OLSRegressionSparse(val xColumnNames: Array[String],
                          val yColumnName: String, 
                          val Xs: CSCMatrix[Double],
                          val Y: breeze.linalg.Vector[Double]
                         ) 
    extends OLSRegression(xColumnNames, yColumnName, Xs, Y) {
  
  // To estimate the intercept, a column of 1's is added to the matrix in the last position
  val XMatrixWith1sColumn = {
    /*val numRows = Y.size
    val numCols = Xs.transpose.apply(0).size
    val ones = List.fill(numRows)(1.0)
    * */
    val numRows = Xs.rows
    val numCols = Xs.cols
    val ones = List.fill(numRows)(1.0)
    
    //new CSCMatrix(numRows, numCols + 1, Xs.flatten.toArray ++ ones)
    //new DenseMatrix(1,1, Array(7))

  }  
    
  val transposedX = XMatrixWith1sColumn.t  
  
  /* Uses pseudoinverse, as it will solve the SinglarMatrixException problem
   *   And all of the jUnit tests still passed, suggesting that the answers
   *   are still comparable to R's
   */
  
  val a = transposedX * XMatrixWith1sColumn
  val inverseOfXtimesXt = pinv(transposedX * XMatrixWith1sColumn)

  /** Standard error for each coefficient; the final entry is for the intercept */
  val standardErrors = {
    val initial = diag(inverseOfXtimesXt).map(math.sqrt(_) * residualStandardError)
    val filtered = initial.map(replaceZero)
    filtered
  }

}*/