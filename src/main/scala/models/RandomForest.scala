package models

import java.io.File

import actors.SeverActor._
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RandomForest {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def randomForest(dtTrainDataPath: String,dataPath:String, name: String,featureSubsetStrategy:String, delimiter: String, numClasses: Int,numTrees:Int, modelResultPath:String,
                  resultPath:String, impurity: String, maxDepth: Int, maxBins: Int): Double = {
    //val conf = new SparkConf().setAppName("RandomForest-" + name).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("RandomForest-" + name).setMaster("yarn-client")//yarn模式
    //val conf = new SparkConf().setAppName("RandomForest-" + name).setMaster("local")//本地模式
    //val sc = new SparkContext(conf)
    conf.setAppName(name)
    val rawData = sc.textFile(dtTrainDataPath)
    val data = rawData.map { line =>
      val values = line.split(delimiter).map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }
    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()
    val model = DecisionTree.trainClassifier(trainData, numClasses, Map[Int, Int](), impurity, maxDepth, maxBins)
    //本地模式删除已存在模型
    if (!modelResultPath.isEmpty) {
      if (conf.get("spark.master") == "local") {
        val file = new File(modelResultPath)
        if (file.exists()) file.delete()
      }
      //集群模式删除已存在模型
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(modelResultPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
      //保存模型
        model.save(sc, modelResultPath)
    }
    val metrics = getMetrics(model, cvData)//用cv集生成度量
    val accuracy = metrics.accuracy//度量在cv集上的总精确度

    /*预测*/
    if (!dataPath.isEmpty){
      val predictData = sc.textFile(dataPath).map{ line =>
        val values = line.split(delimiter).map(_.toDouble)
        val featureVector = Vectors.dense(values.init)
         featureVector
      }
      model.predict(predictData).saveAsTextFile(resultPath)
    }
    //sc.stop()
    accuracy
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {//用cv集产生度量
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }
    new MulticlassMetrics(predictionsAndLabels)
  }



}
