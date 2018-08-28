package seams
/*
import statistics._
import scala.annotation.tailrec
import scala.collection.mutable.HashSet
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast._
import breeze.linalg.SparseVector
import converters.Table
import java.io.File
import org.apache.spark.storage.StorageLevel

class SparseFileData(val sampleNames: Vector[String], val dataPairs: Vector[(String, SparseVector[Double])]) 
  extends FileData(sampleNames, dataPairs)

object SEAMSSparse extends SEAMS {
 
  /** Reads in a file from HDFS converted previously with the ConvertFormat tool */
  def readHDFSFile(filePath: String, spark: SparkContext): SparseFileData = {
    val splitLines = spark.textFile(filePath).map(_.split("\t").toVector)
    val headerLine = splitLines.filter(x => x(0) == "HeaderLine" || x(0) == "Headerline")
      .collect
      .flatten
      .toVector
    val dataLines = splitLines.filter(x => x(0) != "HeaderLine" && x(0) != "Headerline")
      .collect
      .toVector
    // Turns each data line into a tuple where (sampleName, SparseVector[values])
    // Drops the first column because that is the SNP name
    // The :_* unpacks the collection's value to be passed to the SparseVector's constructor one at a time
    val dataTuples = dataLines.map(x => {
      Tuple2(x(0), 
             SparseVector( x.drop(1).map(_.toDouble):_* )
            )  
    })
    new SparseFileData(sampleNames = headerLine.drop(1), dataPairs = dataTuples)
  }
  
}*/