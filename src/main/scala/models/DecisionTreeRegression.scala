package models

import java.io.File

import Util.Util
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object DecisionTreeRegression {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def  decisionTreeRegression(DTRMasterHost: String, DTRMasterPort: String, dtrTrainDataPath: String,predictDataPath:String,
                              modelResultPath: String,predictResultPath:String, DTRName: String,
                              impurity: String, maxDepth: Int, maxBins:Int) ={
    //val conf = new SparkConf().setAppName("decisionTreeRegression-" + DTRName).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("decisionTreeRegression-" + DTRName).setMaster("yarn-client")//yarn模式
    val conf = new SparkConf().setAppName("decisionTreeRegression-" + DTRName).setMaster("local")//本地模式
    val sc = new SparkContext(conf)
    val data = MLUtils.loadLibSVMFile(sc, dtrTrainDataPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val categoricalFeaturesInfo = Map[Int, Int]()


    val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,
      maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelsAndPredictions = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testMSE = labelsAndPredictions.map{ case (v, p) => math.pow(v - p, 2) }.mean()//均方误差

    //本地模式删除已存在模型
    if (!modelResultPath.isEmpty) {
      if (conf.get("spark.master") == "local") {
        val file = new File(modelResultPath)
        if (file.exists()) Util.deleteDir(file)
      }
      //集群模式删除已存在模型
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(modelResultPath)
      hdfs.delete(path, true)
      //保存模型
      model.save(sc, modelResultPath)
    }
    sc.stop()
    testMSE
  }
}
