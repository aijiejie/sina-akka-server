package models

import java.io.File
import java.net.URI

import Util.Util
import actors.RFTaskResult
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import actors.SeverActor._

object DecisonTree {

  Logger.getLogger("org").setLevel(Level.ERROR)
  def decisonTree(dtTrainDataPath: String, dataPath: String, name: String, delimiter: String, numClasses: Int, modelResultPath: String,
                  resultPath: String, impurity: String, maxDepth: Int, maxBins: Int): String = {
    //val conf = new SparkConf().setAppName("DecisionTree-" + name).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("DecisionTree-" + name).setMaster("yarn-client")//yarn模式
    //val conf = new SparkConf().setAppName("DecisionTree-" + name).setMaster("local")//本地模式
    conf.setAppName(name)
    //val sc = new SparkContext(conf)
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
        if (file.exists()) Util.deleteDir(file)
      }
      //集群模式删除已存在模型
      if(conf.get("spark.master") == "spark://master:7077") {
        val hadoopConf = sc.hadoopConfiguration
        val path = new Path(modelResultPath)
        //val fs = org.apache.hadoop.fs.FileSystem.get(URI.create(modelResultPath), hadoopConf)
        val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
        hdfs.delete(path, true)
      }
      //保存模型
      model.save(sc, modelResultPath)
    }

    val metrics = getMetrics(model, cvData)
    //用cv集生成度量
    val accuracy = metrics.accuracy //度量在cv集上的总精确度
    val result = accuracy.toString

    /*预测*/
    if (!dataPath.isEmpty) {
      val predictData = sc.textFile(dataPath).map { line =>
        val values = line.split(delimiter).map(_.toDouble)
        val featureVector = Vectors.dense(values.init)
        featureVector
      }
      model.predict(predictData).saveAsTextFile(resultPath)
    }
    //sc.stop()
    result
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    //用cv集产生度量
    val predictionsAndLabels = data.map { example =>
      (model.predict(example.features), example.label)
    }
    new MulticlassMetrics(predictionsAndLabels)
  }

  /**
    * 打印决策树
    * 目前无法将打印的字符串发送，因为打印后的字符串超过akka大小限制，应该尝试用akka stream解决
    *
    * @param model
    * @return
    */
  def printDecisionTree(model: DecisionTreeModel): String = {
    model.toString() + "\n" +
      printTree(model.topNode)
  }

  def printTree(root: Node): String = {
    val right: String = if (root.rightNode != None) {
      printTree(root.rightNode.get, true, "")
    } else {
      ""
    }

    val rootStr = printNodeValue(root)
    val left: String = if (root.leftNode != None) {
      printTree(root.leftNode.get, false, "")
    } else {
      ""
    }
    right + rootStr + left
  }

  def printNodeValue(root: Node): String = {
    val rootStr: String = if (root.split == None) {
      if (root.isLeaf) {
        root.predict.toString()
      } else {
        ""
      }
    } else {
      "Feature:" + root.split.get.feature + " > " + root.split.get.threshold
    }
    rootStr + "\n"
  }

  def printTree(root: Node, isRight: Boolean, indent: String): String = {
    val right: String = if (root.rightNode != None) {
      printTree(root.rightNode.get, true, indent + (if (isRight) "        " else " |      "))
    } else {
      ""
    }
    //    indent
    val right2 = if (isRight) {
      " /"
    } else {
      " \\"
    }
    val tmp = "----- "
    val rootStr = printNodeValue(root)
    val left: String = if (root.leftNode != None) {
      printTree(root.leftNode.get, false, indent + (if (isRight) " |      " else "        "))
    } else {
      ""
    }
    right + indent + right2 + tmp + rootStr + left
  }

}
