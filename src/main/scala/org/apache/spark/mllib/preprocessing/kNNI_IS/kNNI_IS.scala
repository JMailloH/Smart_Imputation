package org.apache.spark.mllib.preprocessing.kNNI_IS

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ListBuffer
import keel.Dataset.InstanceSet
import keel.Algorithms.Lazy_Learning.LazyAlgorithm
import keel.Algorithms.Lazy_Learning.KNN
import keel.Algorithms.Lazy_Learning.KNN.KNN
import keel.Algorithms.Preprocessing.knnImpute.knnImpute
import utils.Utils
import java.util.ArrayList
import java.util.Arrays
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import keel.Dataset._
import jdk.internal.org.objectweb.asm.tree.IntInsnNode
import scala.io.Source

/**
 * @author Jesus Maillo
 */

class KNNI_IS(train: RDD[String], k: Int, distanceType: Int, header: String, numPartitionMap: Int, numReduces: Int, numIterations: Int, maxWeight: Double, version: String) extends Serializable {

  //Count the samples of each data set and the number of classes
  private val numSamplesTrain = train.count()
  val trainWithIndex = train.zipWithIndex().map(line => (line._2.toInt, line._1)).persist
  val MVs = trainWithIndex.filter(line => line._2.contains("?")).sortByKey().persist
  val notMVs = trainWithIndex.subtractByKey(MVs).map(sample => sample._2.split(","))persist
  private val numSamplesMVs = MVs.count()
  private val numClass = 0 //converter.getNumClassFromHeader()
  private val numFeatures = 0 //converter.getNumFeaturesFromHeader()
  private val headerString: String = Source.fromFile(header).mkString

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

  def getTrain: RDD[String] = train
  def getMVs: RDD[(Int, String)] = MVs
  def getK: Int = k
  def getDistanceType: Int = distanceType
  def getNumPartitionMap: Int = numPartitionMap
  def getNumReduces: Int = numReduces
  def getMaxWeight: Double = maxWeight
  def getNumIterations: Int = numIterations

  //Output
  private var rightPredictedClasses: Array[Array[Array[Int]]] = null

  def getRightPredictedClasses(): Array[Array[Array[Int]]] = {
    rightPredictedClasses
  }

  def getMapTimes(): Array[Double] = {
    mapTimesArray
  }

  def getReduceTimes(): Array[Double] = {
    reduceTimesArray
  }

  def getIterativeTimes(): Array[Double] = {
    iterativeTimesArray
  }

  //private var MVsBroadcast: Broadcast[Array[Array[Double]]] = null

  /**
   * Initial setting necessary.
   */
  def setup(): KNNI_IS = {

    //Setting Iterative MapReduce
    var weightTrain = 0.0
    var weightMVs = 0.0

    if (version == "bag-model") {
      numIter = 1

    } else {
      numIter = 0
      if (numIterations == -1) { //Auto-setting the minimum partition MVs.
        weightTrain = (8 * numSamplesTrain * numFeatures) / (numPartitionMap * 1024.0 * 1024.0)
        weightMVs = (8 * numSamplesMVs * numFeatures) / (1024.0 * 1024.0)
        if (weightTrain + weightMVs < maxWeight * 1024.0) { //It can be run with one iteration
          numIter = 1
        } else {
          if (weightTrain >= maxWeight * 1024.0) {
            println("=> Train wight bigger than lim-task. Abort")
            System.exit(1)
          }
          numIter = (1 + (weightMVs / ((maxWeight * 1024.0) - weightTrain)).toInt)
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

    if (version == "bag-model") {
      timeBegMap = System.nanoTime

      imputedData = train.mapPartitions(split => kNNI_bag(split, atts)).cache
      imputedData.count

      timeEndMap = System.nanoTime
      mapTimesArray(0) = ((timeEndMap - timeBegMap) / 1e9).toDouble
      iterativeTimesArray(0) = ((timeEndMap - timeBegMap) / 1e9).toDouble
      reduceTimesArray(0) = 0.0
    } else {

      //Taking the iterative initial time.
      for (i <- 0 to numIter - 1) {
        timeBegIterative = System.nanoTime

        if (i == numIter - 1) {
          MVsBroadcast = broadcastMVs(MVs.filterByRange(subdel, topdel * 2).collect, sc)
        } else {
          MVsBroadcast = broadcastMVs(MVs.filterByRange(subdel, topdel).collect, sc)
        }

        //Calling KNNI (Map Phase)    
        timeBegMap = System.nanoTime //Taking the map initial time.
        var resultKNNIPartitioned = train.mapPartitions(split => kNNI_exact(split, MVsBroadcast, atts)).cache //.collect
        resultKNNIPartitioned.count
        timeEndMap = System.nanoTime
        mapTimesArray(i) = ((timeEndMap - timeBegMap) / 1e9).toDouble

        //val holita = resultKNNIPartitioned.collect
        //println("\n\nholita size => " + holita.length + "\n\n")
        //for (aux <- holita) { println("@Key => " + aux._1); for (sample <- aux._2) { for (i <- 0 to sample.length - 1) { if (i == 0) { println("\t@Feature => " + sample(i)) } else { println("\t\t@Chicha => " + sample(i)) } }; println() } }

        //Join the result of each split.
        timeBegRed = System.nanoTime //Taking the reduce initial time.
        var resultJoin = resultKNNIPartitioned.reduceByKey(combine(_, _), numReduces)
        resultJoin.count
        timeEndRed = System.nanoTime
        reduceTimesArray(i) = ((timeEndRed - timeBegRed) / 1e9).toDouble

        //Print the middle result
        //for (aux <- resultJoin) { println("Key => " + aux._1); for (sample <- aux._2) { for (i <- 0 to sample.length - 1) { if (i == 0) { println("\tFeature => " + sample(i)) } else { println("\t\tChicha => " + sample(i)) } }; println() } }

        //Impute the dataset with the resultJoin obtained.
        timeBegImpt = System.nanoTime //Taking the imputation initial time.
        val only_imputed = trainWithIndex.join(resultJoin).map(sample => impute(sample, atts.value)) //Missing values has been imputed.
        imputedData = notMVs union only_imputed //Union in order to have all the data set.
        imputedData.count
        timeEndImpt = System.nanoTime
        imputationTimesArray(i) = ((timeEndImpt - timeBegImpt) / 1e9).toDouble

        timeEndIterative = System.nanoTime

        iterativeTimesArray(i) = ((timeEndIterative - timeBegIterative) / 1e9).toDouble
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
   * @brief Calculate the K nearest neighbor from the MVs set over the train set.
   *
   * @param iter Data that iterate the RDD of the train set
   * @param MVsSet The MVs set in a broadcasting
   * @param classes number of label for the objective class
   * @param numNeighbors Value of the K nearest neighbors to calculate
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   */
  def impute(sample: (Int, (String, Array[Array[Float]])), atts: InstanceAttributes): Array[String] = {
    var begTime = System.nanoTime()
    var auxTime = System.nanoTime()

    var Key: Int = sample._1 //Key
    var data: Array[String] = sample._2._1.split(",") //Sample to be imputed
    var infImp: Array[Array[Float]] = sample._2._2 //Information useful to impute
    var numMiss = infImp.length
    var indexMissFeat: Array[Int] = new Array[Int](numMiss) //Index of the missing values.
    //var information: Array[Float] = new Array[Float](numMiss)
    for (x <- 0 until numMiss) {
      indexMissFeat(x) = infImp(x)(0).toInt
    }

    var aux_i = 0
    for (i <- indexMissFeat) {
      var a = atts.getAttribute(i)
      var tipo = a.getType()
      if (tipo != Attribute.NOMINAL) {
        var mean = 0.0
        for (m <- 1 to k) { //Iterate over information 
          mean += infImp(aux_i)(m * 2)
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

        for (m <- 0 until k) {
          if (popMap contains infImp(aux_i)(m * 2)) {
            popMap += (infImp(aux_i)(m * 2) -> (popMap(infImp(aux_i)(m * 2)) + 1))
          } else {
            popMap += (infImp(aux_i)(m * 2) -> 1)
          }
        }

        for (x <- popMap) {
          if (count < x._2) {
            popular = x._1
            count = x._2
          }
        }

        /*
        var count: Int = 1
        var tempCount: Int = 0
        var popular: Float = infImp(0)(2)
        var temp: Float = 0

        for (m <- 1 to k - 1) {
          temp = infImp(aux_i)(m * 2)
          tempCount = 0
          for (m2 <- 2 to k) {
            if (temp == infImp(aux_i)(m2 * 2)) {
              tempCount += 1
            }
          }
          if (tempCount > count) {
            popular = temp
            count = tempCount
          }
        }
				*/
        data(i) = new String(String.valueOf(popular))
      }
      aux_i = aux_i + 1
    }

    data
  }

  /**
   * @brief Calculate the K nearest neighbor from the MVs set over the train set.
   *
   * @param iter Data that iterate the RDD of the train set
   * @param MVsSet The MVs set in a broadcasting
   * @param classes number of label for the objective class
   * @param numNeighbors Value of the K nearest neighbors to calculate
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   */
  def kNNI_bag[T](iter: Iterator[String], atts: Broadcast[InstanceAttributes]): Iterator[Array[String]] = { //Iterator[(Int, Array[Array[Float]])] = { //Si queremos un Pair RDD el iterador seria iter: Iterator[(Long, Array[Double])]

    println("\n\n@entro al map con modelo bolsas\n\n")
    var begTime = System.nanoTime()
    var auxTime = System.nanoTime()
    var auxSet = new ArrayList[String]

    println("@atribute" + atts.value.getAttribute(0))

    //Utils.readHeader(headerString)
    var IS = new InstanceSet()

    var i: Int = 1
    while (iter.hasNext) {
      val cur = iter.next
      auxSet.add(cur.toString())
      i += 1
    }
    //logger.info(auxSet.toString()+"\n")
    IS.readSet(auxSet, false, atts.value)
    println("DATA: Num Instances ---------------------------" + IS.getNumInstances)

    var knni = new knnImpute(IS, k)
    //var imputedData: Array[Array[String]] = new Array[Array[String]](i)
    var imputedData = knni.impute(atts.value)
    /*imputedData(i - 1) = new Array[String](2)
    imputedData(i - 1)(0) = "@time"
    imputedData(i - 1)(1) = ((System.nanoTime - begTime) / 1e9).toFloat.toString()
*/
    imputedData.iterator
  }

  /**
   * @brief Calculate the K nearest neighbor from the MVs set over the train set.
   *
   * @param iter Data that iterate the RDD of the train set
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

    //Utils.readHeader(headerString)
    var IS: InstanceSet = new InstanceSet()
    var IS_MVs: InstanceSet = new InstanceSet()

    var i: Int = 1
    //Parser 
    while (iter.hasNext) {
      val cur = iter.next
      auxSet.add(cur.toString())
      i += 1
    }
    //logger.info(auxSet.toString()+"\n")
    IS.readSet(auxSet, false, atts.value)
    IS_MVs.readSet(MVs_sample, false, atts.value)

    println("DATA: Num Instances ---------------------------" + IS.getNumInstances)

    //println("\nTiempo de lectura => " + ((System.nanoTime() - begTime) / 1e9).toDouble + "\n")
    begTime = System.nanoTime()

    var knni = new knnImpute(IS, IS_MVs, k)
    val imputedData = knni.imputeDistributed(atts.value)
    var indexAux = 0

    println("\nTiempo de imputeDistributed => " + ((System.nanoTime() - begTime) / 1e9).toDouble + "\n")
    begTime = System.nanoTime()
    var res = new Array[(Int, Array[Array[Float]])](imputedData.size)

    for (sample <- imputedData) {
      var i = 0
      var size = sample.size()
      var num_mvs = sample.size() / ((k * 2) + 1)
      while (i < size) {
        val aux = new Array[Array[Float]](num_mvs)
        var j = 0

        while (j < num_mvs) {
          aux(j) = new Array[Float](1 + (k * 2))
          var t = 0
          while (t < (1 + (k * 2))) {

            aux(j)(t) = sample.get(i).toFloat
            t = t + 1
            i = i + 1
          }
          j = j + 1
        }
        res(indexAux) = (keys(indexAux), aux)
        indexAux = indexAux + 1
      }
    }
    //println("\nTiempo de Cambio estructura => " + ((System.nanoTime() - begTime) / 1e9).toDouble + "\n")

    //for (aux <- res) { println("@Key => " + aux._1); for (sample <- aux._2) { for (i <- 0 to sample.length - 1) { if (i == 0) { println("\t@Feature => " + sample(i)) } else { println("\t\t@Chicha => " + sample(i)) } }; println() } }

    res.iterator
  }

  /**
   * @brief Join the result of the map
   *
   * @param mapOut1 A element of the RDD to join
   * @param mapOut2 Another element of the RDD to join
   */
  def combine(mapOut1: Array[Array[Float]], mapOut2: Array[Array[Float]]): Array[Array[Float]] = {
    val num_mvs = mapOut1.length
    var join: Array[Array[Float]] = new Array[Array[Float]](num_mvs)
    var aux: ArrayBuffer[Float] = new ArrayBuffer[Float]

    //Meto todos los elementos de una de las opciones ordenados.
    for (i <- 0 until num_mvs) {
      var itOut1 = 1
      var itOut2 = 1
      aux = new ArrayBuffer[Float]
      aux += mapOut1(i)(0)
      for (j <- 0 until k) {
        var t = 1
        var inserted = false
        var size = aux.length
        while (t < size && !inserted) {
          if (mapOut1(i)(itOut1) < aux(t)) {
            aux.insert(t, mapOut1(i)(itOut1))
            aux.insert(t + 1, mapOut1(i)(itOut1 + 1))
            itOut1 = itOut1 + 2
            t = t + 2
            inserted = true
          } else {
            t = t + 2
          }
        }

        if (!inserted) {
          aux += mapOut1(i)(itOut1)
          aux += mapOut1(i)(itOut1 + 1)
          itOut1 = itOut1 + 2
        }
      }

      for (j <- 0 until k) {
        var t = 1
        var inserted = false
        while (t < ((k * 2) + 1) && !inserted) {
          if (mapOut2(i)(itOut2) < aux(t)) {
            aux.insert(t, mapOut2(i)(itOut2))
            aux.insert(t + 1, mapOut2(i)(itOut2 + 1))
            itOut2 = itOut2 + 2
            t = t + 2
            inserted = true
          } else {
            t = t + 2
          }
        }

        if (inserted) {
          aux.remove((k * 2) + 1, 2)
        }
      }

      join(i) = aux.toArray

    }

    join

    /*val num_mvs = mapOut1.length
    val numNeighbors = k

    var out: Array[ArrayBuffer[Float]] = new Array[ArrayBuffer[Float]](num_mvs)
    var res: Array[Array[Float]] = new Array[Array[Float]](num_mvs)
    for (i <- 0 to num_mvs - 1) {
      out(i) = new ArrayBuffer()
      out(i).insert(0, mapOut1(i)(0))
      var itOut1 = 1
      var itOut2 = 1

      while ((itOut1 < (numNeighbors * 2) - 1) || (itOut2 < (numNeighbors * 2) - 1)) {
        if (itOut1 >= (numNeighbors * 2)) {
          var iter = 1
          var exit = false
          while (iter <= out(i).length && !exit) {
            if (out(i).length == iter) {
              out(i).insert(iter, mapOut2(i)(itOut2))
              out(i).insert(iter + 1, mapOut2(i)(itOut2 + 1))
              exit = true
            } else if (out(i)(iter) > mapOut2(i)(itOut2)) {
              out(i).insert(iter, mapOut2(i)(itOut2))
              out(i).insert(iter + 1, mapOut2(i)(itOut2 + 1))
              exit = true
            }
            iter = iter + 2
          }
          itOut2 = itOut2 + 2

        } else if (itOut2 >= (numNeighbors * 2)) {
          var iter = 1
          var exit = false
          while (iter <= out(i).length && !exit) {
            if (out(i).length == iter) {
              out(i).insert(iter, mapOut1(i)(itOut1))
              out(i).insert(iter + 1, mapOut1(i)(itOut1 + 1))
              exit = true
            } else if (out(i)(iter) > mapOut1(i)(itOut1)) {
              out(i).insert(iter, mapOut1(i)(itOut1))
              out(i).insert(iter + 1, mapOut1(i)(itOut1 + 1))
              exit = true
            }
            iter = iter + 2
          }
          itOut1 = itOut1 + 2
        } else if (mapOut1(i)(itOut1) < mapOut2(i)(itOut2)) {
          //Insertar mirando desde el principio y desplazando los demas.
          var iter = 1
          var exit = false
          while (iter <= out(i).length && !exit) {
            if (out(i).length == iter) {
              out(i).insert(iter, mapOut1(i)(itOut1))
              out(i).insert(iter + 1, mapOut1(i)(itOut1 + 1))
              exit = true
            } else if (out(i)(iter) > mapOut1(i)(itOut1)) {
              out(i).insert(iter, mapOut1(i)(itOut1))
              out(i).insert(iter + 1, mapOut1(i)(itOut1 + 1))
              exit = true
            }
            iter = iter + 2
          }
          itOut1 = itOut1 + 2
        } else {
          //Insertar mirando desde el principio y desplazando los demas.
          var iter = 1
          var exit = false
          while (iter <= out(i).length && !exit) {
            if (out(i).length == iter) {
              out(i).insert(iter, mapOut2(i)(itOut2))
              out(i).insert(iter + 1, mapOut2(i)(itOut2 + 1))
              exit = true
            } else if (out(i)(iter) > mapOut2(i)(itOut2)) {
              out(i).insert(iter, mapOut2(i)(itOut2))
              out(i).insert(iter + 1, mapOut2(i)(itOut2 + 1))
              exit = true
            }
            iter = iter + 2
          }
          itOut2 = itOut2 + 2
        }
      }
    }

    for (i <- 0 to num_mvs - 1) {
      res(i) = new Array[Float]((numNeighbors * 2) + 1)
      res(i) = out(i).take((numNeighbors * 2) + 1).toArray
    }
    res*/

    /*
    //Print the middle result
    for (sample <- out) {
      for (i <- 0 to sample.length - 1) {
        if (i == 0) {
          println("\tFeature => " + sample(i))
        } else {
          println("\t\tChicha => " + sample(i))
        }
      }
      println()
    }
		*/

    /*
    //Print the middle result
    for (sample <- out) {
      for (i <- 0 to sample.length - 1) {
        if (i == 0) {
          println("\tFeature => " + sample(i))
        } else {
          println("\t\tChicha => " + sample(i))
        }
      }
      println()
    }
		*/

    /*
    val num_mvs = mapOut1.length

    for (i <- 0 until num_mvs) {
      var itOut1 = 1
      var itOut2 = 1
      for (j <- 0 until k) {
        if (mapOut1(i)(itOut1) <= mapOut2(i)(itOut2)) {
          mapOut2(i)(itOut2) = mapOut1(i)(itOut1)
          mapOut2(i)(itOut2 + 1) = mapOut1(i)(itOut1 + 1)
          itOut2 = itOut2 + 2
        } else {
          itOut1 = itOut1 + 2
        }
      }
    }

    mapOut2*/
  }

}

object KNNI_IS {
  /**
   * Initial setting necessary.
   *
   * @param train Data that iterate the RDD of the train set
   * @param MVs The MVs set in a broadcasting
   * @param k number of neighbors
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2 ; HVDM = 3
   * @param converter Dataset's information read from the header
   * @param numPartitionMap Number of partition. Number of map tasks
   * @param numReduces Number of reduce tasks
   * @param numIterations Autosettins = -1. Number of split in the MVs set and number of iterations
   */
  def setup(train: RDD[String], k: Int, distanceType: Int, header: String, numPartitionMap: Int, numReduces: Int, numIterations: Int, maxWeight: Double, version: String) = {
    new KNNI_IS(train, k, distanceType, header, numPartitionMap, numReduces, numIterations, maxWeight, version).setup()
  }
}