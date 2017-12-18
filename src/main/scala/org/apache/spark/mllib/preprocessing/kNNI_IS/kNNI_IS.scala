package org.apache.spark.mllib.preprocessing.kNNI_IS

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ListBuffer
import keel.Dataset.InstanceSet
import keel.Algorithms.Preprocessing.knnImpute.knnImpute
import utils.keel.Utils
import java.util.ArrayList
import java.util.Arrays
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import keel.Dataset._
import jdk.internal.org.objectweb.asm.tree.IntInsnNode
import scala.io.Source

/**
 * @author Jesus Maillo
 *
 * Impute with k Nearest Neighbors Imputation methods with approximate or original approach.
 * @param data dataset with missing values
 * @param k the number of neighbors to consider while the imputation
 * @param distanceType Euclidean or Manhattan
 * @param header String to the header file
 * @param numPartitionMap Number of map tasks
 * @param numIterations Number of iteration
 * @param maxWeight Maximum weight of each partition
 * @param version Approximate or exact approach
 */

class KNNI_IS(data: RDD[String], k: Int, distanceType: Int, header: String, numPartitionMap: Int, numIterations: Int, maxWeight: Double, version: String) extends Serializable {

  //Count the samples of each data set and the number of classes
  private val numSamplesdata = data.count()
  val dataWithIndex = data.zipWithIndex().map(line => (line._2.toInt, line._1)).persist
  val MVs = dataWithIndex.filter(line => line._2.contains("?")).sortByKey().persist
  val notMVs = dataWithIndex.subtractByKey(MVs).map(sample => sample._2.split(","))persist
  private val numSamplesMVs = MVs.count()
  private val numClass = 0
  private val numFeatures = 0

  var atts: InstanceAttributes = null

  //Setting Iterative MapReduce
  private var inc = 0
  private var subdel = 0
  private var topdel = 0
  private var numIter = numIterations
  private def broadcastMVs(MVs: Array[(Int, String)], context: SparkContext) = context.broadcast(MVs)
  private def broadcastAtts(atts: InstanceAttributes, context: SparkContext) = context.broadcast(atts)

  //Time variables
  private val timeBeg = System.nanoTime
  private var timeEnd: Long = 0
  private var mapTimesArray: Array[Double] = null
  private var reduceTimesArray: Array[Double] = null
  private var imputationTimesArray: Array[Double] = null
  private var iterativeTimesArray: Array[Double] = null

  //Output variables
  private var imputedData: RDD[Array[String]] = null
  private var rightPredictedClasses: Array[Array[Array[Int]]] = null

  // Getters
  def getdata: RDD[String] = data
  def getMVs: RDD[(Int, String)] = MVs
  def getK: Int = k
  def getDistanceType: Int = distanceType
  def getNumPartitionMap: Int = numPartitionMap
  def getMaxWeight: Double = maxWeight
  def getNumIterations: Int = numIterations
  def getRightPredictedClasses(): Array[Array[Array[Int]]] = { rightPredictedClasses }
  def getMapTimes(): Array[Double] = { mapTimesArray }
  def getReduceTimes(): Array[Double] = { reduceTimesArray }
  def getIterativeTimes(): Array[Double] = { iterativeTimesArray }
  def getImputedData(): RDD[Array[String]] = {imputedData}

  /**
   * Basic initialization.
   */
  def setup(): KNNI_IS = {

    //Setting Iterative MapReduce
    var weightdata = 0.0
    var weightMVs = 0.0

    if (version == "local") {
      numIter = 1
    } else {
      numIter = 0
      if (numIterations == -1) { //Auto-setting the minimum partition MVs.
        weightdata = (8 * numSamplesdata * numFeatures) / (numPartitionMap * 1024.0 * 1024.0)
        weightMVs = (8 * numSamplesMVs * numFeatures) / (1024.0 * 1024.0)
        if (weightdata + weightMVs < maxWeight * 1024.0) { //It can be run with one iteration
          numIter = 1
        } else {
          if (weightdata >= maxWeight * 1024.0) {
            println("=> data wight bigger than lim-task. Abort")
            System.exit(1)
          }
          numIter = (1 + (weightMVs / ((maxWeight * 1024.0) - weightdata)).toInt)
        }

      } else {
        numIter = numIterations
      }
    }

    println("=> NumberIterations \"" + numIter + "\"")

    inc = (numSamplesMVs / numIter).toInt
    subdel = 0
    topdel = inc
    if (numIterations == 1) { //If only one partition
      topdel = numSamplesMVs.toInt + 1
    }

    mapTimesArray = new Array[Double](numIter)
    reduceTimesArray = new Array[Double](numIter)
    iterativeTimesArray = new Array[Double](numIter)
    imputationTimesArray = new Array[Double](numIter)
    rightPredictedClasses = new Array[Array[Array[Int]]](numIter)

    this

  }

  /**
   * Imputation.
   */
  def imputation(sc: SparkContext): RDD[Array[String]] = {
    var MVsBroadcast: Broadcast[Array[(Int, String)]] = null
    var MVsBroadcast_OLD: Broadcast[Array[String]] = null
    var resultKNNPartitioned: RDD[(Int, Array[Array[Float]])] = null
    var timeBegMap = 0.0.toLong
    var timeEndMap = 0.0.toLong
    var timeBegRed = 0.0.toLong
    var timeEndRed = 0.0.toLong
    var timeBegImpt = 0.0.toLong
    var timeEndImpt = 0.0.toLong
    var timeBegIterative = 0.0.toLong
    var timeEndIterative = 0.0.toLong

    var atts = broadcastAtts(Utils.readHeaderFromFile(header), sc)

    if (version == "local") {
      timeBegMap = System.nanoTime

      imputedData = data.mapPartitions(split => kNNI_approximate(split, atts)).cache
      imputedData.count

      timeEndMap = System.nanoTime
      mapTimesArray(0) = ((timeEndMap - timeBegMap) / 1e9).toDouble
      iterativeTimesArray(0) = ((timeEndMap - timeBegMap) / 1e9).toDouble
      reduceTimesArray(0) = 0.0
    } else {

      //Taking the iterative initial time.
      for (i <- 0 until numIter) {
        timeBegIterative = System.nanoTime
        
        if (i == numIter - 1) {
          MVsBroadcast = broadcastMVs(MVs.filterByRange(subdel, topdel * 2).collect, sc)
        } else {
          MVsBroadcast = broadcastMVs(MVs.filterByRange(subdel, topdel).collect, sc)
        }

        //Impute the dataset with the  resultJoin obtained.
        if (imputedData == null) {
          imputedData = (notMVs union (dataWithIndex.join(data.mapPartitions(split => kNNI_exact(split, MVsBroadcast, atts)).reduceByKey(combine(_, _))).map(sample => impute(sample, atts.value)))).cache
        } else {
          imputedData = imputedData union (dataWithIndex.join(data.mapPartitions(split => kNNI_exact(split, MVsBroadcast, atts)).reduceByKey(combine(_, _))).map(sample => impute(sample, atts.value))).cache
        }
        imputedData.count
        //Update the pairs of delimiters
        subdel = topdel + 1
        topdel = topdel + inc + 1
        MVsBroadcast.destroy
      }

    }
    timeEnd = System.nanoTime
    imputedData
  }

  /**
   * Write the results in HDFS
   *
   */
  def writeResults(sc: SparkContext, pathOutput: String, data: Boolean, times: Boolean) = {
    //Write the dataset imputed
    if (data) {
      imputedData.map(sample => sample.mkString(",")).repartition(1).saveAsTextFile(pathOutput)
    }

    if (times) {
      //Write the Times file
      var writerTimes = new ListBuffer[String]
      writerTimes += "***Times.txt ==> Contain: run maps time; run reduce time; run iterative time; Total Runtimes***"
      var sumTimesAux = 0.0
      for (i <- 0 to numIter - 1) {
        sumTimesAux = sumTimesAux + mapTimesArray(i)
      }
      writerTimes += "\n@mapTime\n" + sumTimesAux / numIter.toDouble
      sumTimesAux = 0.0
      for (i <- 0 to numIter - 1) {
        sumTimesAux = sumTimesAux + reduceTimesArray(i)
      }
      writerTimes += "\n@reduceTime\n" + sumTimesAux / numIter.toDouble
      sumTimesAux = 0.0
      for (i <- 0 to numIter - 1) {
        sumTimesAux = sumTimesAux + imputationTimesArray(i)
      }
      writerTimes += "\n@imputationTime\n" + sumTimesAux / numIter.toDouble
      sumTimesAux = 0.0
      for (i <- 0 to numIter - 1) {
        sumTimesAux = sumTimesAux + iterativeTimesArray(i)
      }
      writerTimes += "\n@iterativeTime\n" + sumTimesAux / numIter.toDouble

      writerTimes += "\n@totalRunTime\n" + (timeEnd - timeBeg) / 1e9

      val timesTxt = sc.parallelize(writerTimes, 1)
      timesTxt.saveAsTextFile(pathOutput + "/Times.txt")
    }
  }

  /**
   * @brief Missing values has imputed with the information of the map.
   */
  def impute(sample: (Int, (String, Array[Array[Float]])), atts: InstanceAttributes): Array[String] = {
    var begTime = System.nanoTime()
    var auxTime = System.nanoTime()

    var Key: Int = sample._1 //Key
    var data: Array[String] = sample._2._1.split(",") //Sample to be imputed
    var infImp: Array[Array[Float]] = sample._2._2 //Information useful to impute
    var numMiss = infImp.length - 1
    var indexMissFeat: Array[Int] = new Array[Int](numMiss) //Index of the missing values.
    //var information: Array[Float] = new Array[Float](numMiss)
    for (x <- 0 until numMiss) {
      indexMissFeat(x) = infImp(x + 1)(0).toInt
    }

    var aux_i = 1
    for (i <- indexMissFeat) {
      var a = atts.getAttribute(i)
      var tipo = a.getType()
      if (tipo != Attribute.NOMINAL) {
        var mean = 0.0
        for (m <- 1 to k) { //Iterate over information 
          mean += infImp(aux_i)(m)
        }

        if (tipo == Attribute.INTEGER)
          mean = Math.round(mean)

        if (mean < a.getMinAttribute()) {
          mean = a.getMinAttribute()
        } else if (mean > a.getMaxAttribute()) {
          mean = a.getMaxAttribute()
        }

        if (tipo == Attribute.INTEGER) {
          data(i) = new String(String.valueOf(mean.toInt))
        } else {
          data(i) = new String(String.valueOf(mean))
        }
      } else {

        var popMap = scala.collection.mutable.HashMap.empty[Float, Int]
        var popular: Float = 0.0.toFloat
        var count: Int = 0

        for (m <- 1 to k) {
          if (popMap contains infImp(aux_i)(m)) {
            popMap += (infImp(aux_i)(m) -> (popMap(infImp(aux_i)(m)) + 1))
          } else {
            popMap += (infImp(aux_i)(m) -> 1)
          }
        }

        for (x <- popMap) {
          if (count < x._2) {
            popular = x._1
            count = x._2
          }
        }

        data(i) = new String(String.valueOf(popular))
      }
      aux_i = aux_i + 1
    }

    data
  }

  /**
   * @brief Calculate the K nearest neighbor from the MVs set over the data set.
   *
   * @param iter Data that iterate the RDD of the data set
   * @param MVsSet The MVs set in a broadcasting
   * @param classes number of label for the objective class
   * @param numNeighbors Value of the K nearest neighbors to calculate
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   */
  def kNNI_approximate[T](iter: Iterator[String], atts: Broadcast[InstanceAttributes]): Iterator[Array[String]] = { //Iterator[(Int, Array[Array[Float]])] = { //Si queremos un Pair RDD el iterador seria iter: Iterator[(Long, Array[Double])]

    var begTime = System.nanoTime()
    var auxTime = System.nanoTime()
    var auxSet = new ArrayList[String]

    println("@atribute" + atts.value.getAttribute(0))

    var IS = new InstanceSet()

    var i: Int = 1
    while (iter.hasNext) {
      val cur = iter.next
      auxSet.add(cur.toString())
      i += 1
    }
    IS.readSet(auxSet, false, atts.value)
    println("DATA: Num Instances ---------------------------" + IS.getNumInstances)

    var knni = new knnImpute(IS, k)
    var imputedData = knni.impute(atts.value)

    imputedData.iterator
  }

  /**
   * @brief Calculate the K nearest neighbor from the MVs set over the data set.
   *
   * @param iter Data that iterate the RDD of the data set
   * @param MVsSet The MVs set in a broadcasting
   * @param classes number of label for the objective class
   * @param numNeighbors Value of the K nearest neighbors to calculate
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   */
  def kNNI_exact[T](iter: Iterator[String], MVs: Broadcast[Array[(Int, String)]], atts: Broadcast[InstanceAttributes]): Iterator[(Int, Array[Array[Float]])] = { //Iterator[(Int, Array[Array[Float]])] = { //Si queremos un Pair RDD el iterador seria iter: Iterator[(Long, Array[Double])]
    var begTime = System.nanoTime()
    var auxTime = System.nanoTime()
    var auxSet = new ArrayList[String]
    val keys: Array[Int] = MVs.value.map(_._1)
    val MVs_sample: Array[String] = MVs.value.map(_._2)

    var IS: InstanceSet = new InstanceSet()
    var IS_MVs: InstanceSet = new InstanceSet()

    var i: Int = 1
    //Parser 
    while (iter.hasNext) {
      val cur = iter.next
      auxSet.add(cur.toString())
      i += 1
    }
    IS.readSet(auxSet, false, atts.value)
    IS_MVs.readSet(MVs_sample, false, atts.value)

    println("DATA: Num Instances ---------------------------" + IS.getNumInstances)

    begTime = System.nanoTime()

    var knni = new knnImpute(IS, IS_MVs, k)
    val imputedData = knni.imputeDistributed(atts.value)
    var indexAux = 0

    begTime = System.nanoTime()
    var res = new Array[(Int, Array[Array[Float]])](imputedData.size)

    for (sample <- imputedData) {
      val size = sample.size()
      var num_mvs = (size - k) / (k + 1)

      val aux = new Array[Array[Float]](num_mvs + 1)
      aux(0) = new Array[Float](k)
      var i = 0
      while (i < k) {
        aux(0)(i) = sample.get(i).toFloat
        i = i + 1
      }

      var j = 1
      while (i < size) {
        aux(j) = new Array[Float](k + 1)
        var t = 0
        while (t < k + 1) {
          aux(j)(t) = sample.get(i).toFloat
          t = t + 1
          i = i + 1
        }
        j = j + 1
      }
      res(indexAux) = (keys(indexAux), aux)
      indexAux = indexAux + 1
    }

    res.iterator
  }

  /**
   * @brief Join the result of the map
   *
   * @param mapOut1 A element of the RDD to join
   * @param mapOut2 Another element of the RDD to join
   */
  def combine(mapOut1: Array[Array[Float]], mapOut2: Array[Array[Float]]): Array[Array[Float]] = {
    val num_mvs = mapOut1.length - 1 //First array is about the distances
    var result: Array[Array[Float]] = new Array[Array[Float]](num_mvs + 1)

    var join: Array[ArrayBuffer[Float]] = new Array[ArrayBuffer[Float]](num_mvs + 1)
    join(0) = new ArrayBuffer[Float]
    for (i <- 1 to num_mvs) {
      join(i) = new ArrayBuffer[Float]
      join(i).append(mapOut1(i)(0)) //MVs index as first element
    }

    //Sorting all the candidates of mapOut1
    for (i <- 0 until k) {
      var inserted = false
      //aux = new ArrayBuffer[Float]
      var t = 0
      var size = join(0).length
      while (t < size && !inserted) {
        if (mapOut1(0)(i) < join(0)(t)) {
          join(0).insert(t, mapOut1(0)(i)) //Distance
          for (l <- 1 to num_mvs) { //Values for this neigh and all the MVs of this instances
            join(l).insert(t + 1, mapOut1(l)(i + 1))
          }
          inserted = true
        }
        t = t + 1
      }

      if (!inserted) {
        join(0) += mapOut1(0)(i) //Distance
        for (l <- 1 to num_mvs) { //Value for this neigh
          join(l) += mapOut1(l)(i + 1)
        }
      }

    }

    //Sort and combine the candidates from mapOut2
    for (i <- 0 until k) {
      var inserted = false
      //aux = new ArrayBuffer[Float]
      var t = 0
      var size = join(0).length
      while (t < size && !inserted) {
        if (mapOut2(0)(i) < join(0)(t)) {
          join(0).insert(t, mapOut2(0)(i)) //Distance
          for (l <- 1 to num_mvs) { //Values for this neigh and all the MVs of this instances
            join(l).insert(t + 1, mapOut2(l)(i + 1))
          }
          inserted = true
        }
        t = t + 1
      }

      if (inserted) {
        join(0).remove(k) //Distance
        for (l <- 1 to num_mvs) { //Value for this neigh
          join(l).remove(k + 1)
        }
      }

    }
    var joinLenght = join.length
    for (i <- 0 until joinLenght) {
      result(i) = join(i).toArray
    }

    result
  }

}

object KNNI_IS {
  /**
   * Initial setting necessary.
   *
   * @param data Data that iterate the RDD of the data set
   * @param k number of neighbors
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   * @param header as an string in keel format
   * @param numPartitionMap Number of partition. Number of map tasks
   * @param version 'local' or 'global'
   */
  def setup(data: RDD[String], k: Int, distanceType: Int, header: String, numPartitionMap: Int, version: String) = {
    new KNNI_IS(data, k, distanceType, header, numPartitionMap, 1, 25, version).setup()
  }
}