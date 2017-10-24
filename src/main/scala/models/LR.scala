package models

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

object LR {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def lr(lrMasterHost: String, lrMasterPort: String, lrTrainDataPath: String,
         lrPredictDataPath: String, lrModelResultPath:String,lrPredictResultPath: String, lrName: String,
         numIterations: Int) ={
    //val conf = new SparkConf().setAppName("LR-" + lrName).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar")).set("fs.defaultFS", "hdfs://master:8020")//集群模式java -jar提交
    //val conf = new SparkConf().setAppName("LR-" + lrName).setMaster("yarn-client")//yarn模式
    val conf = new SparkConf().setAppName("LR-" + lrName).setMaster("local")//本地模式
    val sc = new SparkContext(conf)

    // $example on$
    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc,lrTrainDataPath)
    //val stdData = standardization(data)//标准化标签为0，1的数据集


    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(numIterations)
      .run(training)


    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")


    //本地模式删除已存在模型
    if (!lrModelResultPath.isEmpty) {
      if (conf.get("spark.master") == "local") {
        val file = new File(lrModelResultPath)
        if (file.exists()) file.delete()
      }
      //集群模式删除已存在模型
      val hadoopConf = sc.hadoopConfiguration
      hadoopConf.addResource(new Path("core-site.xml"))
      hadoopConf.addResource(new Path("hdfs-site.xml"))
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(lrModelResultPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
      //保存模型
      model.save(sc, lrModelResultPath)
    }
    //预测模型
    if (!lrPredictDataPath.isEmpty){
      val data = MLUtils.loadVectors(sc,lrTrainDataPath)
      model.predict(data).saveAsTextFile(lrPredictResultPath)
    }

    sc.stop()
    accuracy
  }

}
