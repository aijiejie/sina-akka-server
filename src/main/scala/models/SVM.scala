package models

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import actors.SeverActor._

object SVM {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def svm(svmMasterHost:String,svmMasterPort:String,svmTrainDataPath:String,
          svmPredictDataPath:String,svmModelResultPath:String,svmPredictResultPath:String,
          name:String,iter:Int) = {
    //val conf = new SparkConf().setAppName("SVM-" + name).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("SVM-" + name).setMaster("yarn-client")//yarn模式
    //val conf = new SparkConf().setAppName("SVM-" + name).setMaster("local")//本地模式
    //val sc = new SparkContext(conf)
    conf.setAppName("SVM-" + name)//本地模式

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc,svmTrainDataPath)
    //val stdData = standardization(data)//标准化标签为0，1的数据集


    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = iter
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()
    //metrics.roc().foreach(x=>println(x._1 + "," + x._2))

    println("Area under ROC = " + auROC)

    //本地模式删除已存在模型
    if (!svmModelResultPath.isEmpty) {
      if (conf.get("spark.master") == "local") {
        val file = new File(svmModelResultPath)
        if (file.exists()) file.delete()
      }
      //集群模式删除已存在模型
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(svmModelResultPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
      //保存模型
      model.save(sc, svmModelResultPath)
    }
    //预测模型
    if (!svmPredictDataPath.isEmpty){
      val data = MLUtils.loadVectors(sc,svmTrainDataPath)
      model.predict(data).saveAsTextFile(svmPredictResultPath)
    }

    //sc.stop()
    auROC
  }


  //将标签为1，2的标准化为0，1
  def standardization(rdd:RDD[LabeledPoint]): RDD[LabeledPoint] ={
    val newRdd = rdd.map(x=>{
      val label = x.label-1
      LabeledPoint(label,x.features)
    })
    newRdd
  }
}
