package statistics

case class ANOVARow(sampleName: String,
                    df: Int,
                    SS: Double,
                    MS: Double,
                    F: Option[Double],
                    p_value: Option[Double]
                   )

/**
  * For now, this is a placeholder class. It is just something for ANOVATableDense to inherit from
  *
  * If we pursue the sparse matrix implementation, it will need its own ANOVA table maker
  * (This is because creating the ANOVA table's SS values for each predictor requires taking apart matrices by removing
  *   one column
  *
  *   In model Y ~ A + B + C,
  *
  *   SS(A) = SS(A, B, C) - SS(B, C)
  *
  *   The way to calculate this requires removing a single column from the original input matrix, creating a new
  *     OLSRegression model, and grabbing the SS field. The implementation of this changes fundamentally depending
  *     on whether the matrix is dense or sparse.
  * )
  */
abstract class ANOVATable {

  val table: Vector[ANOVARow]
  def printTable(): Unit

}


