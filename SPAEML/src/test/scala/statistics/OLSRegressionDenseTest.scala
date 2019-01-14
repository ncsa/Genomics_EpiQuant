package statistics

import org.junit.Assert._
import org.junit._
import breeze.linalg.{DenseMatrix, DenseVector}

object OLSRegressionDenseTest {
  val DELTA = 1e-5
  
  var simpleReg: OLSRegression = _
  var multiReg: OLSRegression = _

  @BeforeClass
  def initialSetup(): Unit = {

    /*
     *  We found that if we use really small data sets (N < 5) some calculations give results that do not match
     *    R's output (logLik, AIC, etc.)
     *
     *  Decided we should use data sets where N >= 30
     */

    val X1 = Array(27.0, 24, 9, 13, 10, 7, 1, 23, 14, 26,
                   3, 15, 24, 15, 13, 21, 17, 1, 18, 17,
                   22, 14, 7, 5, 12, 22, 26, 19, 13, 6
                  )
    val X2 = Array(23.0, 4, 19, 19, 3, 17, 13, 23, 4, 14,
                   15, 15, 18, 23, 23, 18, 23, 5, 7, 12,
                    2, 30, 12, 12, 15, 18, 17, 22, 6, 13
                  )
    val X3 = Array(3.0, 16, 19, 9, 20, 17, 20, 21, 14, 4,
                   5, 15, 8, 30, 12, 19, 23, 2, 1, 21,
                   2, 14, 22, 23, 25, 8, 7, 2, 6, 29
                  )

    val multiX: Array[Double] = Vector(X1, X2, X3).flatten.toArray

    val matrixSingleX = new DenseMatrix(X1.length, 1, X1)
    val matrixMultiX = new DenseMatrix(X1.length, 3, multiX)

    val Y = DenseVector(1.0, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                        11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30
                       )

    simpleReg = new OLSRegression(Array("X1"), "Y", matrixSingleX, Y)
    multiReg = new OLSRegression(Array("X1", "X2", "X3"), "Y", matrixMultiX, Y)

  }
}

class OLSRegressionDenseTest {

  /*
   * SIMPLE LINEAR REGRESSION TESTS
   * 
   * Need to agree with the following R output

# Code
x <- c(27.0, 24, 9, 13, 10, 7, 1, 23, 14, 26, 3, 15, 24, 15, 13, 21, 17, 1, 18, 17, 22, 14, 7, 5, 12, 22, 26, 19, 13, 6)
y <- c(1.0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)
model <- lm(y~x)

summary(model)

Call:
lm(formula = y ~ x)

Residuals:
    Min      1Q  Median      3Q     Max
-13.857  -6.936   0.116   6.931  14.036

Coefficients:
            Estimate Std. Error t value Pr(>|t|)
(Intercept) 16.28041    3.59851   4.524 0.000102 ***
x           -0.05273    0.21663  -0.243 0.809462
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Residual standard error: 8.95 on 28 degrees of freedom
Multiple R-squared:  0.002112,	Adjusted R-squared:  -0.03353
F-statistic: 0.05925 on 1 and 28 DF,  p-value: 0.8095

anova(model)

Analysis of Variance Table

Response: y
          Df  Sum Sq Mean Sq F value Pr(>F)
x          1    4.75   4.746  0.0592 0.8095
Residuals 28 2242.75  80.098

logLik(model)
'log Lik.' -107.2821 (df=3)

AIC(model)
[1] 220.5642

BIC(model)
[1] 224.7678

Add up Sum Sq column to get SST
SST = 2247.50

*/
  
  @Test
  def simpleCoefficientsTest(): Unit = {
    val coefficients = OLSRegressionDenseTest.simpleReg.coefficients
    
    // Two estimates should be present: one for the slope and one for the intercept
    assertEquals(coefficients.length, 2)
    val X1 = coefficients(0)
    val intercept = coefficients(1)

    assertEquals(-0.05273, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(16.28041, intercept, OLSRegressionDenseTest.DELTA)
  }
  
  @Test
  def simpleStandardErrorTest(): Unit = {
    val stdErrors = OLSRegressionDenseTest.simpleReg.standardErrors
    
    assertEquals(stdErrors.length, 2)
    val X1 = stdErrors(0)
    val intercept = stdErrors(1)

    assertEquals(0.21663, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(3.59851, intercept, OLSRegressionDenseTest.DELTA)
  }
  
  @Test
  def simpleTStatisticTest(): Unit = {
    val tStats = OLSRegressionDenseTest.simpleReg.tStatistics
    
    assertEquals(tStats.length, 2)
    val X1 = tStats(0)
    val intercept = tStats(1)

    assertEquals(-0.243, X1, 0.001)
    assertEquals(4.524, intercept, 0.001)
  }
  
  @Test
  def simplePValueTest(): Unit = {
    val pVals = OLSRegressionDenseTest.simpleReg.pValues
    
    assertEquals(pVals.length, 2)
    val X1 = pVals(0)
    val intercept = pVals(1)

    assertEquals(0.809462, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(0.000102, intercept, OLSRegressionDenseTest.DELTA)
  }

  @Test
  def simpleRSquaredTest(): Unit = {
    assertEquals(0.002112, OLSRegressionDenseTest.simpleReg.R_squared, OLSRegressionDenseTest.DELTA)
  }

  @Test
  def simpleAdjustedRSquaredTest(): Unit = {
    assertEquals(-0.03353, OLSRegressionDenseTest.simpleReg.adjusted_R_squared, OLSRegressionDenseTest.DELTA)
  }

  @Test
  def simpleLogLikelihoodTest(): Unit = {
    assertEquals(-107.2821, OLSRegressionDenseTest.simpleReg.log_likelihood, OLSRegressionDenseTest.DELTA)
  }

  @Test
  def simpleAICTest(): Unit =  assertEquals(220.5642, OLSRegressionDenseTest.simpleReg.AIC, 0.0001)
  @Test
  def simpleBICTest(): Unit = assertEquals(224.7678, OLSRegressionDenseTest.simpleReg.BIC, 0.0001)

  @Test
  def simpleSumOfSquaredResidualsTest(): Unit = assertEquals(2242.75, OLSRegressionDenseTest.simpleReg.RSS, 0.01)
  @Test
  def simpleSumOfSquaredTotalTest(): Unit = assertEquals(2247.50, OLSRegressionDenseTest.simpleReg.SST, 0.01)


  /*
   * MULTIPLE LINEAR REGRESSION TESTS
   * 
   * Should agree with the following R output

x1 <- c(27.0, 24, 9, 13, 10, 7, 1, 23, 14, 26, 3, 15, 24, 15, 13, 21, 17, 1, 18, 17, 22, 14, 7, 5, 12, 22, 26, 19, 13, 6)
x2 <- c(23.0, 4, 19, 19, 3, 17, 13, 23, 4, 14, 15, 15, 18, 23, 23, 18, 23, 5, 7, 12, 2, 30, 12, 12, 15, 18, 17, 22, 6, 13)
x3 <- c(3.0, 16, 19, 9, 20, 17, 20, 21, 14, 4,  5, 15, 8, 30, 12, 19, 23, 2, 1, 21, 2, 14, 22, 23, 25, 8, 7, 2, 6, 29)
y <- c(1.0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30)

model2 <- lm(y~x1+x2+x3)

summary(model2)

Call:
lm(formula = y ~ x1 + x2 + x3)

Residuals:
     Min       1Q   Median       3Q      Max
-14.0236  -6.7252   0.1896   7.0406  14.3608

Coefficients:
             Estimate Std. Error t value Pr(>|t|)
(Intercept) 16.830240   5.898178   2.853  0.00837 **
x1          -0.062707   0.246841  -0.254  0.80146
x2          -0.001349   0.252293  -0.005  0.99577
x3          -0.027493   0.217198  -0.127  0.90025
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Residual standard error: 9.285 on 26 degrees of freedom
Multiple R-squared:  0.00278,	Adjusted R-squared:  -0.1123
F-statistic: 0.02416 on 3 and 26 DF,  p-value: 0.9948

anova(model2)
Analysis of Variance Table

Response: y
          Df  Sum Sq Mean Sq F value Pr(>F)
x1         1    4.75   4.746  0.0551 0.8163
x2         1    0.12   0.120  0.0014 0.9705
x3         1    1.38   1.381  0.0160 0.9002
Residuals 26 2241.25  86.202

logLik(model2)
'log Lik.' -107.2721 (df=5)

AIC(model2)
[1] 224.5441

BIC(model2)
[1] 231.5501

Add up Sum Sq column to get SST
SST = 2247.50
   */
  
  @Test
  def multiCoefficientsTest(): Unit = {
    val coefficients = OLSRegressionDenseTest.multiReg.coefficients
    
    // Four estimates should be present: three slopes and one for the intercept
    assertEquals(coefficients.length, 4)
    val X1 = coefficients(0)
    val X2 = coefficients(1)
    val X3 = coefficients(2)
    val intercept = coefficients(3)

    assertEquals(-0.062707, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(-0.001349, X2, OLSRegressionDenseTest.DELTA)
    assertEquals(-0.027493, X3, OLSRegressionDenseTest.DELTA)
    assertEquals(16.830240, intercept, OLSRegressionDenseTest.DELTA)
  }
  
  @Test
  def multiStandardErrorTest(): Unit = {
    val stdErr = OLSRegressionDenseTest.multiReg.standardErrors
    
    assertEquals(stdErr.length, 4)
    val X1 = stdErr(0)
    val X2 = stdErr(1)
    val X3 = stdErr(2)
    val intercept = stdErr(3)

    assertEquals(0.246841, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(0.252293, X2, OLSRegressionDenseTest.DELTA)
    assertEquals(0.217198, X3, OLSRegressionDenseTest.DELTA)
    assertEquals(5.898178, intercept, OLSRegressionDenseTest.DELTA)
  }
    
  @Test
  def multiTStatisticTest():Unit = {
    val tStats = OLSRegressionDenseTest.multiReg.tStatistics
    
    assertEquals(tStats.length, 4)
    val X1 = tStats(0)
    val X2 = tStats(1)
    val X3 = tStats(2)
    val intercept = tStats(3)

    assertEquals(-0.254, X1, 0.001)
    assertEquals(-0.005, X2, 0.001)
    assertEquals(-0.127, X3, 0.001)
    assertEquals(2.853, intercept, 0.001)
  }
  
  @Test
  def multiPValueTest():Unit = {
    val pVals = OLSRegressionDenseTest.multiReg.pValues
    
    assertEquals(pVals.length, 4)
    val X1 = pVals(0)
    val X2 = pVals(1)
    val X3 = pVals(2)
    val intercept = pVals(3)

    assertEquals(0.80146, X1, OLSRegressionDenseTest.DELTA)
    assertEquals(0.99577, X2, OLSRegressionDenseTest.DELTA)
    assertEquals(0.90025, X3, OLSRegressionDenseTest.DELTA)
    assertEquals(0.00837, intercept, OLSRegressionDenseTest.DELTA)
  }

  @Test
  def multiRSquaredTest(): Unit = assertEquals(0.00278, OLSRegressionDenseTest.multiReg.R_squared, 0.0001)

  @Test
  def multiAdjustedRSquaredTest(): Unit = {
    assertEquals(-0.1123, OLSRegressionDenseTest.multiReg.adjusted_R_squared, 0.0001)
  }

  @Test
  def multiLogLikelihoodTest(): Unit = {
    assertEquals(-107.2721, OLSRegressionDenseTest.multiReg.log_likelihood, 0.0001)
  }

  @Test
  def multiAICTest(): Unit = assertEquals(224.5441, OLSRegressionDenseTest.multiReg.AIC, 0.0001)
  @Test
  def multiBICTest(): Unit = assertEquals(231.5501, OLSRegressionDenseTest.multiReg.BIC, 0.0001)

  @Test
  def multiSumOfSquaredResidualsTest(): Unit = assertEquals(2241.25, OLSRegressionDenseTest.multiReg.RSS, 0.01)
  @Test
  def multiSumOfSquaredTotalTest(): Unit = assertEquals(2247.50, OLSRegressionDenseTest.multiReg.SST, 0.01)


}