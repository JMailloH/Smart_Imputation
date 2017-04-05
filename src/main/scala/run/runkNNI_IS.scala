package org.apache.spark.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.preprocessing.kNNI_IS.KNNI_IS
import org.apache.log4j.Logger
//import utils.parserKeel.KeelParser
import utils.keel.KeelParser
import keel.Dataset
import java.util.Scanner
import keel.Dataset.InstanceSet
import scala.collection.mutable.ArrayBuffer

object runkNNI_IS extends Serializable {

  var sc: SparkContext = null

  def main(arg: Array[String]) {

    var logger = Logger.getLogger(this.getClass())

    if (arg.length == 8) {
      logger.error("=> wrong parameters number")
      System.err.println("Parameters \n\t<path-to-header>\n\t<path-to-train>\n\t<number-of-neighbors>\n\t<number-of-partition>\n\t<number-of-reducers-task>\n\t<path-to-output>\n\t<maximum-weight-per-task>")
      System.exit(1)
    }

    //Reading parameters
    val pathHeader = arg(0)
    val pathTrain = arg(1)
    val K = arg(2).toInt
    val distanceType = 2
    val numPartitionMap = arg(3).toInt
    val numReduces = arg(4).toInt
    val numIterations = arg(5).toInt //if == -1, auto-setting
    val pathOutput = arg(6)
    var maxWeight = 0.0
    if (numIterations == -1) {
      maxWeight = arg(7).toDouble
    }
    val version = arg(8)

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
    logger.info("=> NumberReducePartition \"" + numReduces + "\"")
    logger.info("=> DistanceType \"" + distanceType + "\"")
    logger.info("=> pathToOuput \"" + pathOutput + "\"")
    if (numIterations == -1) {
      logger.info("=> maxWeight \"" + maxWeight + "\"")
    }
    logger.info("=> version \"" + version + "\"")

    //Reading header of the dataset and dataset
    val data = sc.textFile(pathTrain: String, numPartitionMap).persist

    println("data count => " + data.count)

    val knni = KNNI_IS.setup(data, K, distanceType, pathHeader, numPartitionMap, numReduces, numIterations, maxWeight, version)
    val imputedData = knni.imputation(sc)
    
    //Write the result: data, times.
    knni.writeResults(sc, pathOutput, data = true, times = true)
  }

}
