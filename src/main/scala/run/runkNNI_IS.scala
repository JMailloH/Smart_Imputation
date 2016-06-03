package org.apache.spark.run

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.preprocessing.kNNI_IS.KNNI_IS
import org.apache.log4j.Logger
//import utils.parserKeel.KeelParser
import utils.KeelParser
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
    val jobName = "Maillo - kNNI_IS -> " + outDisplay + " K = " + K

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

    //val typeConversion = KeelParser.parseHeaderFile(sc, pathHeader)
    //val bcTypeConv = sc.broadcast(typeConversion)
    //val testRaw = sc.textFile(pathTest: String, numPartitionMap)
    //val MVsRaw = testRaw.map(line => KeelParser.parseLabeledPoint(bcTypeConv.value, line)).cache
    //MVsRaw.count
    //println("\n\n\n" + MVsRaw.first() + "\n\n\n")

    /*
      val parsed = converter.parserToDouble(line)
      val featureVector = Vectors.dense(parsed.init)
      val label = parsed.last
      LabeledPoint(label, featureVector)
    }.persist

    */

    //Reading header of the dataset and dataset
    val converter = new KeelParser(sc, pathHeader)
    val data = sc.textFile(pathTrain: String, numPartitionMap).persist
    //val header = sc.broadcast(sc.textFile(pathHeader: String, 1).collect)

    val knni = KNNI_IS.setup(data, K, distanceType, pathHeader, numPartitionMap, numReduces, numIterations, maxWeight, version)
    val imputedData = knni.imputation(sc)
    //Write the result: data, times.
    knni.writeResults(sc, pathOutput, data = true, times = true)

    /*println("\n\n@Local Web UI at: http://localhost:4040/jobs/\n\tPress any key to end ...")
    var in = ""
    var inScanner = new Scanner(System.in)
    in = inScanner.nextLine()*/
    /*
    if (version == "mllib") {

    } else {
      val sqlContext = new org.apache.spark.sql.SQLContext(train.context)
      import sqlContext.implicits._

      var outPathArray: Array[String] = new Array[String](1)
      outPathArray(0) = pathOutput
      val knn = new org.apache.spark.ml.classification.kNNI_IS.kNNI_ISClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setK(K)
        .setDistanceType(distanceType)
        .setConverter(converter)
        .setNumPartitionMap(numPartitionMap)
        .setNumReduces(numReduces)
        .setNumIter(numIterations)
        .setMaxWeight(maxWeight)
        .setNumSamplesTest(test.count.toInt)
        .setOutPath(outPathArray)

      // Chain indexers and tree in a Pipeline
      val pipeline = new Pipeline().setStages(Array(knn))

      // Train model.  This also runs the indexers.
      val model = pipeline.fit(train.toDF())
      val predictions = model.transform(test.toDF())
    }
*/ }

}
