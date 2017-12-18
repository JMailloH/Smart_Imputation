package org.apache.spark.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.preprocessing.kNNI_IS.KNNI_IS
import org.apache.log4j.Logger
import utils.keel.KeelParser
import keel.Dataset
import java.util.Scanner
import keel.Dataset.InstanceSet
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

object runSmart_Imputation extends Serializable {

  var sc: SparkContext = null

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length == 5) {
      logger.error("=> wrong parameters number")
      System.err.println("Parameters \n\t<path-to-header>\n\t<path-to-train>\n\t<number-of-neighbors>\n\t<number-of-partition>\n\t<version-global-local>\n\t<path-to-output>")
      System.exit(1)
    }

    //Reading parameters
    val pathHeader = arg(0)
    val pathTrain = arg(1)
    val K = arg(2).toInt
    val distanceType = 2
    val numPartitionMap = arg(3).toInt
    val version = arg(4)
    val pathOutput = arg(5)

    //Clean pathOutput for set the jobName
    var outDisplay: String = pathOutput

    //Basic setup
    val nameDataset = pathTrain.substring(1 + pathTrain.lastIndexOf("/"), pathTrain.length - 7)
    val jobName = "kNNI_IS => Dataset: " + nameDataset + " K: " + K + " Mode: " + version

    //Spark Configuration
    val conf = new SparkConf().setAppName(jobName)
    sc = new SparkContext(conf)

    logger.info("=> jobName \"" + jobName + "\"")
    logger.info("=> pathToHeader \"" + pathHeader + "\"")
    logger.info("=> pathToTrain \"" + pathTrain + "\"")
    logger.info("=> NumberNeighbors \"" + K + "\"")
    logger.info("=> NumberMapPartition \"" + numPartitionMap + "\"")
    logger.info("=> DistanceType \"" + distanceType + "\"")
    logger.info("=> pathToOuput \"" + pathOutput + "\"")
    logger.info("=> version \"" + version + "\"")

    var timeBeg: Long = 0l
    var timeEnd: Long = 0l

    //Reading header of the dataset and dataset
    timeBeg = System.nanoTime
    val data = sc.textFile(pathTrain: String, numPartitionMap).persist
    val knni = KNNI_IS.setup(data, K, distanceType, pathHeader, numPartitionMap, version)
    val imputedData = knni.imputation(sc)
    timeEnd = System.nanoTime

    //Write the result: data, times.
    var writerReport = new ListBuffer[String]
    writerReport += "***Report.txt ==> Contain: Imputation Runtime***\n"
    writerReport += "@MembershipRuntime\n" + (timeEnd - timeBeg) / 1e9
    val Report = sc.parallelize(writerReport, 1)
    Report.saveAsTextFile(pathOutput + "/Report.txt")
    imputedData.map(sample => sample.mkString(",")).repartition(1).saveAsTextFile(pathOutput+ "/Data")
  }

}
