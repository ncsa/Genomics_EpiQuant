package statistics

import breeze.linalg.DenseMatrix
import breeze.stats.distributions.{FDistribution}

import scala.collection.mutable.ArrayBuffer

class ANOVATableDense(regression: OLSRegressionDense) extends ANOVATable {

  /*
  Building the anova table
            df          SS      MS      F*    p-value
  error     n - 1 - 3                   --    --
  x1        1
  x2        1
  x3        1
  Total     n - 1

  SS(x1) = SS(x1,x2,x3) - SS(x2,x3)
  df is 1 for all terms that are not related categorically (for now, just hard code the 1 value in, although
    later, we need to parse the predictor names for categorical info (maybe encode the info as <SNPName>%%%<category>
    or something)

  F* = MS_marker_effect / MS_error


  Currently calculates SS and MS correctly when there is only 1 predictor, but not otherwise

  */

  private val SST = regression.SST
  private val dof_error = regression.DoF_error
  private val dof_model = regression.DoF_model
  private val dof_total = regression.N - 1

  def FStatisticToPValue(F: Double, df_num: Int, df_denom: Int): Double = {
    1 - new FDistribution(df_num, df_denom).cdf(math.abs(F)) * 2
  }

  /**
    * Returns a matrix with all but one column still present
    */
  private def matrixExcludeOneColumn(m: DenseMatrix[Double], excludedColumn: Int): DenseMatrix[Double] = {
    var values: ArrayBuffer[Double] = ArrayBuffer()
    (0 until m.cols).foreach( i => {
      // The function <matrix>(::, <index>) extracts a column
      // Add the values of each of those columns to the ListBuffer if it's not the column we want to ignore
      if (i != excludedColumn) values ++= m(::, i).toArray
    })
    new DenseMatrix(m.rows, m.cols - 1, values.toArray)
  }

  private def createANOVAPredictorRow(columnIndex: Int): ANOVARow = {

    val sampleName = regression.xColumnNames(columnIndex)
    // In the future, this could be changed to determine if multiple columns are under the same category
    val df = 1

    // Create regression with all terms EXCEPT this term
    val xsAllButOne = matrixExcludeOneColumn(regression.Xs, columnIndex)

    val xColumnNamesAllButOne = {
      val names = regression.xColumnNames
      val left = names.slice(0, columnIndex)
      val right = names.slice(columnIndex + 1, names.length)
      left ++ right
    }

    val regressionAllButOne = {
      new OLSRegressionDense(xColumnNamesAllButOne, regression.yColumnName, xsAllButOne, regression.Y)
    }

    val SS = regression.total_SS_model - regressionAllButOne.total_SS_model
    val MS = SS / df
    val F =  MS / regression.total_MS_model
    val p_value = FStatisticToPValue(F, df, regression.DoF_model)

    ANOVARow(sampleName, df, SS, MS, Option(F), Option(p_value))
  }

  // VERIFIED
  private val anovaErrorRow = {
    val SS = regression.RSS
    ANOVARow("Error", dof_error, SS, SS / dof_error, None, None)
  }

  private val anovaTotalRow = {
    val SS = regression.SST
    val F = regression.F_statistic_model
    val p_value = FStatisticToPValue(F, dof_model, regression.DoF_error)
    ANOVARow("Total", dof_total, SS, SS / dof_model, Option(F), Option(p_value))
  }

  lazy val table: Vector[ANOVARow] = {
    val arrayBuffer: ArrayBuffer[ANOVARow] = ArrayBuffer(anovaErrorRow)

    for (i <- 0 until regression.Xs.cols) { arrayBuffer += createANOVAPredictorRow(i) }
    arrayBuffer += anovaTotalRow

    arrayBuffer.toVector
  }

  private def anovaRowToString(row: ANOVARow): String = {
    val outString: StringBuilder = new StringBuilder(row.sampleName + "\t" + row.df + "\t")

    val SS = row.SS
    val MS = row.MS

    outString.append(f"$SS%.4f".toString + "\t")
    outString.append(f"$MS%.4f".toString + "\t")

    row.F match {
      case Some(f_value) => outString.append(f"$f_value%.5f".toString + "\t")
      case None => outString.append("-\t")
    }
    row.p_value match {
      case Some(p_value) => outString.append(f"$p_value%.5f".toString + "\t")
      case None => outString.append("-")
    }
    outString.toString()
  }

  def printTable(): Unit = {
    println("-\tdf\tSS\tMS\tF")
    table.foreach(row => println(anovaRowToString(row)) )
  }
}
