package actors
import akka.actor.{Actor, Props}
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import models.DecisonTree

import scala.concurrent.duration._
import scala.concurrent.Await

class Supervisor extends Actor {



  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: ArithmeticException => Resume
      case _: NullPointerException => Restart
      case _: IllegalArgumentException => Stop
      case _: java.net.URISyntaxException => Stop
      case _: Exception => Stop
    }

  def receive = {
    case p: Props => {
      val server = context.actorOf(p, "Server")
      println(server.path)
    }
    case DTTask(masterHost, masterPort, dtTrainDataPath, dataPath, modelResultPath, resultPath,
    numClasses, name, impurity, maxDepth, maxBins, delimiter) => {
      val alsActor = context.actorOf(Props[AlsActor],name)
      alsActor.forward(DTTask(masterHost, masterPort, dtTrainDataPath, dataPath, modelResultPath, resultPath,
        numClasses, name, impurity, maxDepth, maxBins, delimiter))
    }
    case "stop" =>{
      context.children.foreach(a => a ! "Kill")
    }

  }
}


//Als算法actor
class AlsActor extends Actor{
  override def receive: Receive = {
    case DTTask(masterHost, masterPort,dtTrainDataPath, dataPath, modelResultPath, resultPath,numClasses, name, impurity, maxDepth,maxBins ,delimiter) => {
      sender() ! "收到决策树任务"
      /*下面的函数调用以后要改用子actor实现，与主actor隔离，并实行监控，
      一旦spark任务出错，可以想办法用父actor来结束子actor的spark任务*/
      import scala.concurrent.ExecutionContext.Implicits.global
      context.system.scheduler.schedule(5.minutes,0.seconds,self,"kill")
      val result = DecisonTree.decisonTree(dtTrainDataPath, dataPath, name,
      delimiter, numClasses, modelResultPath, resultPath, impurity, maxDepth, maxBins)
      sender() ! DTTaskResult(modelResultPath, result.toString, resultPath)
    }


  }
}

object AlsActor{
  def props = Props[AlsActor]
}