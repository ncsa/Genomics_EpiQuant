package lasso

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LassoWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import spaeml.{DenseFileData, SPAEMLDense}

object LASSO {

  def train(genoTypeFile: String, phenoTypeFile: String, spark: SparkContext): Unit = {

    val geno = SPAEMLDense.readHDFSFile(genoTypeFile, spark)
    val pheno = SPAEMLDense.readHDFSFile(phenoTypeFile, spark)

    val rdd = createRDD(geno, pheno, spark)
    val model = LassoWithSGD.train(rdd, 100)

    val valuesAndPreds = rdd.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }.collect()

    println(rdd.count())
    println(geno.dataPairs.size)
    println(pheno.dataPairs.size)
    println(valuesAndPreds.mkString)
  }

  def createRDD(geno: DenseFileData, pheno: DenseFileData, spark: SparkContext): RDD[LabeledPoint] = {

    // class DenseFileData(sampleNames: Vector[String], dataPairs: Vector[(String, DenseVector[Double])])

    val zippedData = geno.

    val labledPoints = zippedData.map {
      row => LabeledPoint(row._1._2(0), Vectors.dense(row._2._2.toArray))
    }

    spark.parallelize(labledPoints).cache()
  }

}