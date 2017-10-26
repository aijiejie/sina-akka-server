package models

import java.io.File

import Util.Util
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.{SparkConf, SparkContext}

object LinerRegression {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def LinerRegression(linerRMasterHost: String, linerRMasterPort: String, linerRTrainDataPath: String,
                      linerRPredictDataPath: String, linerRModelResultPath: String, linerRPredictResultPath: String,
                      linerRName: String, numIterations: Int, delimiter:String, stepSize:BigDecimal) = {

    //val conf = new SparkConf().setAppName("LinerRegression-" + linerRName).setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("LinerRegression-" + linrRName).setMaster("yarn-client")//yarn模式
    val conf = new SparkConf().setAppName("LinerRegression-" + linerRName).setMaster("local")//本地模式
    val sc = new SparkContext(conf)

    val data = sc.textFile(linerRTrainDataPath)
    val parsedData = data.map { line =>
      val parts = line.split(delimiter)
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize.toDouble)
    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow(v - p, 2) }.mean()

    //本地模式删除已存在模型
    if (!linerRModelResultPath.isEmpty) {
      if (conf.get("spark.master") == "local") {
        val file = new File(linerRModelResultPath)
        if (file.exists()) Util.deleteDir(file)
      }
      //集群模式删除已存在模型
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(linerRModelResultPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
      //保存模型
      model.save(sc, linerRModelResultPath)
    }
    sc.stop()
    MSE
  }
}
