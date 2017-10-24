import actors._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import models._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.duration._

class SeverActor() extends Actor {
  Logger.getLogger("org").setLevel(Level.ERROR)

  override def receive: Receive = {
    case "connect" => {
      println("receive task")
      sender ! "connect ok"
    }
    //停止命令
    case "stop" => context.system.terminate()
    //测试响应
    case ClientSubmitTask(dataPath, name) => {
      sender ! "收到测试任务"
      val result = SeverActor.run(dataPath, name)
      sender ! TestResult(result)
    }

    case _: String => {
      println("无效命令")
    }

    //Als应答
    case AlsTask(masterHost, masterPort, datapath, dataResultPath, alsRseultNumber, name, rank, iter, delimiter) => {
      sender ! "收到ALS算法任务"
      /*下面的函数调用以后要改用子actor实现，与主actor隔离，并实行监控，一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
      val result = SeverActor.Als(datapath, name, dataResultPath, alsRseultNumber, rank, iter, delimiter)
      if (result) sender ! "ALS推荐算法任务成功结束"
    }
      //决策树应答
    case DTTask(masterHost, masterPort,dtTrainDataPath, dataPath, modelResultPath, resultPath,numClasses, name, impurity, maxDepth,maxBins ,delimiter) => {
      sender ! "收到决策树任务"
      /*下面的函数调用以后要改用子actor实现，与主actor隔离，并实行监控，一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
      val result = DecisonTree.decisonTree(dtTrainDataPath, dataPath, name, delimiter, numClasses, modelResultPath, resultPath, impurity, maxDepth, maxBins)
      sender ! DTTaskResult(modelResultPath, result, resultPath)
    }
    //随机森林任务
    case RFTask(masterHost, masterPort, rfTrainData,predictData, modelResult,presictResult,
    numClasses,numTrees ,name,featureSubsetStrategy, impurity, maxDepth,maxBins ,delimiter) => {
      sender ! "收到随机森林任务"
      val result = RandomForest.randomForest(rfTrainData, predictData, name,featureSubsetStrategy, delimiter, numClasses,numTrees, modelResult, presictResult, impurity, maxDepth, maxBins)
      sender ! RFTaskResult(modelResult,result.toString,presictResult)
    }
      //SVM任务
    case SvmTask(svmMasterHost,svmMasterPort,svmTrainDataPath,
    svmPredictDataPath,svmModelResultPath,svmPredictResultPath,
    name,iter) =>{
      sender ! "收到SVM任务"
      val result =SVM.svm(svmMasterHost,svmMasterPort,svmTrainDataPath,
        svmPredictDataPath,svmModelResultPath,svmPredictResultPath,
        name,iter)
      sender ! SvmTaskResult(result,svmModelResultPath,svmPredictResultPath)
    }
      //LR任务
    case LRTask(lrMasterHost,lrMasterPort,lrTrainDataPath,
    lrPredictDataPath,lrModelResultPath,lrPredictResultPath,
    name,iter) =>{
      sender ! "收到LR任务"
      val result = LR.lr(lrMasterHost,lrMasterPort,lrTrainDataPath,
        lrPredictDataPath,lrModelResultPath,lrPredictResultPath,
        name,iter)
      sender ! LRTaskResult(result,lrModelResultPath,lrPredictResultPath)
    }
    //决策树回归应答
    case DTRTask(masterHost, masterPort, dtTrainData, predictData, modelResult, result
    , name, impurity, maxDepth, maxBins) => {
      sender ! "收到决策树回归任务"
      /*下面的函数调用以后要改用子actor实现，与主actor隔离，并实行监控，一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
      val testMSE = DecisionTreeRegression.decisionTreeRegression(masterHost, masterPort, dtTrainData, predictData, modelResult, result
        , name, impurity, maxDepth, maxBins)
      sender ! DTRTaskResult(modelResult, testMSE.toString, result)
    }
      //线性回归应答
    case LinerRegressionTask(linerRMasterHost, linerRMasterPort, linerRTrainDataPath,
    linerRPredictDataPath, linerRModelResultPath, linerRPredictResultPath,
    linerRname, iter,delimiter,stepSize) =>{
      sender ! "收到线性回归任务"
      val mse = LinerRegression.LinerRegression(linerRMasterHost, linerRMasterPort, linerRTrainDataPath,
        linerRPredictDataPath, linerRModelResultPath, linerRPredictResultPath,
        linerRname, iter,delimiter,stepSize)
      sender ! LinerRegressionTaskResult(mse,linerRModelResultPath,linerRPredictResultPath)
    }
  }
}

object SeverActor {
  var serverActor: ActorRef = _

  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = args(1)

    val cf=ConfigFactory.load().getConfig("master")


//    val configStr:String =
//      s"""
//         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
//         |akka.remote.netty.tcp.hostname = "$host"
//         |akka.remote.netty.tcp.port = "$port"
//       """.stripMargin
//    val mlconfig = ConfigFactory.parseString(configStr)

    val actorSystem = ActorSystem.create("MasterActor",cf)//使用master进行序列化

    //val supervisor = actorSystem.actorOf(Props[Supervisor],"Supervisor")//监控server测试
    //supervisor ! Props(new SeverActor())
    serverActor = actorSystem.actorOf(Props(new SeverActor()), "Server")
    print(serverActor.path)
    //Await.ready(actorSystem.whenTerminated, 30.minutes)
    //actorSystem.awaitTermination(30.minutes)
  }


/*测试模块*/
  def run(dataPath: String, name: String): String = {
    val logFile = dataPath
    val conf = new SparkConf().setAppName(name).setMaster("spark://master:7077")
    //val conf = new SparkConf().setAppName(name).setMaster("local")//.setJars(List("C:\\Users\\少辉\\Desktop\\新浪实习\\serverActor\\target\\scala-2.11\\serverActor-assembly-1.0.jar"))
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val numAs = logData.filter(line => line.contains("-")).count()
    val numBs = logData.filter(line => line.contains("+")).count()
    println("Lines with -: %s, Lines with +: %s".format(numAs, numBs))
    sc.stop()
    "Lines with -: %s, Lines with +: %s".format(numAs, numBs)
  }
/*ALS模块*/
  def Als(dataPath: String, name: String, dataResultPath: String, alsResultNumber: Int, rank: Int, numIterations: Int, delimiter: String): Boolean = {
    val conf = new SparkConf().setAppName("ALS").setMaster("spark://master:7077")
    //val conf = new SparkConf().setAppName("ALS").setMaster("yarn-client")
    //val conf = new SparkConf().setAppName("ALS" + name).setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile(dataPath)
    val ratings = data.map(_.split(delimiter) match { //处理数据
      case Array(user, item, rate) => //将数据集转化
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
    allRs.coalesce(1).sortByKey().saveAsTextFile(dataResultPath)
    sc.stop()
    true
  }


}