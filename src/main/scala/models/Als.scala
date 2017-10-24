package models

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, Rating}

object Als {
  Logger.getLogger("org").setLevel(Level.ERROR)

  /*ALS模块*/
  def als(dataPath: String, name: String, dataResultPath: String, alsResultNumber: Int, rank: Int, numIterations: Int, delimiter: String): Boolean = {
    //val conf = new SparkConf().setAppName("ALS").setMaster("spark://master:7077")//.setJars(Seq("/home/hadoop/spark-app/app-jar/play/serverActor-assembly-2.6.jar"))//集群模式
    //val conf = new SparkConf().setAppName("ALS").setMaster("yarn-client")//yarn模式
    val conf = new SparkConf().setAppName("ALS" + name).setMaster("local")//本地模式
    val sc = new SparkContext(conf)
    val data = sc.textFile(dataPath)
    val ratings = data.map(_.split(delimiter) match { //处理数据
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    }) //将数据集转化为专用Rating
    val model = ALS.train(ratings, rank, numIterations, 0.01) //进行模型训练
    //model.save(sc, "myAls") //保存模型
    val allRs = model.recommendProductsForUsers(alsResultNumber). //为所有用户推荐n个商品
      map {
      case (userID, recommendations) => {
        var recommendationStr = ""
        for (r <- recommendations) {
          recommendationStr += "产品" + r.product + "评分:" + r.rating + ","
        }
        if (recommendationStr.endsWith(","))
          recommendationStr = recommendationStr.substring(0, recommendationStr.length - 1)

        ("用户" + userID, "推荐：" + recommendationStr)
      }
    }
    //删除已存在结果文件
    if (conf.get("spark.master") == "local") {
      val file = new File(dataResultPath)
      if (file.exists()) file.delete()
    }else {
      val hadoopConf = sc.hadoopConfiguration
      val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
      val path = new Path(dataResultPath)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true)
      }
    }
    allRs.coalesce(1).sortByKey().saveAsTextFile(dataResultPath)//保存预测结果
    sc.stop()
    true
  }
}
