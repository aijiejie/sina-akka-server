package actors

import akka.actor.{Actor, ActorRef, Props}
import models._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

class SeverActor() extends Actor {
  Logger.getLogger("org").setLevel(Level.ERROR)

  /*下面的函数调用以后要改用子actor实现，与主actor隔离实现并行，并实行监控，
  一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
  override def receive: Receive = {
    case "connect" => {
      println("have client")
      sender() ! "connect ok"
    }
    //停止命令
    case "stop" => context.system.terminate()
    //测试任务
    case ClientSubmitTask(dataPath, name) => {
      sender() ! "收到测试任务"
      val result = SeverActor.run(dataPath, name)
      sender() ! TestResult(result)
    }

    case _: String => {
      println("无效命令")
    }

    //Als任务
    case AlsTask(masterHost, masterPort, datapath, dataResultPath, alsRseultNumber, name, rank, iter, delimiter) => {
      sender() ! "收到ALS算法任务"
      val result = Als.als(datapath, name, dataResultPath, alsRseultNumber, rank, iter, delimiter)
      if (result) sender() ! "ALS推荐算法任务成功结束"
    }
      //决策树任务函数调用改akka
    case DTTask(masterHost, masterPort,dtTrainDataPath, dataPath, modelResultPath,
    resultPath,numClasses, name, impurity, maxDepth,maxBins ,delimiter) => {
      //sender() ! "收到决策树任务"
      val DTActor = context.actorOf(Props[DTActor],"DTActor")//创建子Actor
      DTActor forward DTTask(masterHost, masterPort, dtTrainDataPath, dataPath, modelResultPath, resultPath,
        numClasses, name, impurity, maxDepth, maxBins, delimiter)//转发消息
      //val result = DecisonTree.decisonTree(dtTrainDataPath, dataPath, name, delimiter, numClasses, modelResultPath, resultPath, impurity, maxDepth, maxBins)
      //sender() ! DTTaskResult(modelResultPath, result, resultPath)
    }
    //随机森林任务函数改akka
    case RFTask(masterHost, masterPort, rfTrainData,predictData, modelResult,predictResult,
    numClasses,numTrees ,name,featureSubsetStrategy, impurity, maxDepth,maxBins ,delimiter) => {
      val RFActor = context.actorOf(Props[RFActor],"RFActor")
      RFActor forward RFTask(masterHost, masterPort, rfTrainData,predictData, modelResult,predictResult,
        numClasses,numTrees ,name,featureSubsetStrategy, impurity, maxDepth,maxBins ,delimiter)
//      sender() ! "收到随机森林任务"
//      val result = RandomForest.randomForest(rfTrainData, predictData, name,featureSubsetStrategy, delimiter, numClasses,numTrees, modelResult, predictResult, impurity, maxDepth, maxBins)
//      sender() ! RFTaskResult(modelResult,result.toString,predictResult)
    }
      //SVM任务函数调用改akka
    case SvmTask(svmMasterHost,svmMasterPort,svmTrainDataPath,
    svmPredictDataPath,svmModelResultPath,svmPredictResultPath,
    name,iter) =>{
      val svmActor = context.actorOf(Props[SvmActor],"svmActor")//创建子Actor
      svmActor forward SvmTask(svmMasterHost, svmMasterPort, svmTrainDataPath,
        svmPredictDataPath, svmModelResultPath, svmPredictResultPath,
        name, iter)//转发消息
      //sender() ! "收到SVM任务"
      //      val result =SVM.svm(svmMasterHost,svmMasterPort,svmTrainDataPath,
//        svmPredictDataPath,svmModelResultPath,svmPredictResultPath,
//        name,iter)
      //sender() ! SvmTaskResult(result,svmModelResultPath,svmPredictResultPath)
    }
      //LR任务
    case LRTask(lrMasterHost,lrMasterPort,lrTrainDataPath,
    lrPredictDataPath,lrModelResultPath,lrPredictResultPath,
    name,iter) =>{
      sender() ! "收到LR任务"
      val result = LR.lr(lrMasterHost,lrMasterPort,lrTrainDataPath,
        lrPredictDataPath,lrModelResultPath,lrPredictResultPath,
        name,iter)
      sender() ! LRTaskResult(result,lrModelResultPath,lrPredictResultPath)
    }
    //决策树回归任务
    case DTRTask(masterHost, masterPort, dtTrainData, predictData, modelResult, result
    , name, impurity, maxDepth, maxBins) => {
      sender() ! "收到决策树回归任务"
      /*下面的函数调用以后要改用子actor实现，与主actor隔离，并实行监控，一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
      val testMSE = DecisionTreeRegression.decisionTreeRegression(masterHost, masterPort, dtTrainData, predictData, modelResult, result
        , name, impurity, maxDepth, maxBins)
      sender() ! DTRTaskResult(modelResult, testMSE.toString, result)
    }
      //线性回归任务
    case LinerRegressionTask(linerRMasterHost, linerRMasterPort, linerRTrainDataPath,
    linerRPredictDataPath, linerRModelResultPath, linerRPredictResultPath,
    linerRname, iter,delimiter,stepSize) =>{
      sender() ! "收到线性回归任务"
      val mse = LinerRegression.LinerRegression(linerRMasterHost, linerRMasterPort, linerRTrainDataPath,
        linerRPredictDataPath, linerRModelResultPath, linerRPredictResultPath,
        linerRname, iter,delimiter,stepSize)
      sender() ! LinerRegressionTaskResult(mse,linerRModelResultPath,linerRPredictResultPath)
    }
  }
}

object SeverActor {
  var serverActor: ActorRef = _
  //将sparkContext设为全局变量
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ML").set("spark.scheduler.mode", "FAIR")
  val sc = new SparkContext(conf)
//  def main(args: Array[String]): Unit = {
//
//    val host = args(0)
//    val port = args(1)
//    val configStr =
//      s"""
//         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
//         |akka.remote.netty.tcp.hostname = "$host"
//         |akka.remote.netty.tcp.port = "$port"
//       """.stripMargin
//    val config = ConfigFactory.parseString(configStr)
//    val actorSystem = ActorSystem("MasterActor", config)
//    //val supervisor = actorSystem.actorOf(Props[Supervisor],"Supervisor")//监控server测试
//    //supervisor ! Props(new actors.SeverActor())
//
//    serverActor = actorSystem.actorOf(Props(new actors.SeverActor()), "Server")
//    Await.ready(actorSystem.whenTerminated, 30.minutes)
//  }


/*测试模块*/
  def run(dataPath: String, name: String): String = {
    val logFile = dataPath
    //val conf = new SparkConf().setAppName(name).setMaster("spark://master:7077")
    conf.setAppName(name)
    //val conf = new SparkConf().setAppName(name).setMaster("local")//.setJars(List("C:\\Users\\少辉\\Desktop\\新浪实习\\serverActor\\target\\scala-2.11\\serverActor-assembly-1.0.jar"))
    //val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val numAs = logData.filter(line => line.contains("-")).count()
    val numBs = logData.filter(line => line.contains("+")).count()
    println("Lines with -: %s, Lines with +: %s".format(numAs, numBs))
    //sc.stop()
    "Lines with -: %s, Lines with +: %s".format(numAs, numBs)
  }



}