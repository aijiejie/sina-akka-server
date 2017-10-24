package actors

import akka.actor.Actor
import models.DecisonTree

class DTActor extends Actor{
  override def receive = {
    case DTTask(masterHost, masterPort,dtTrainDataPath, dataPath, modelResultPath, resultPath,numClasses, name, impurity, maxDepth,maxBins ,delimiter) => {
      sender() ! "收到决策树任务"
      val result = DecisonTree.decisonTree(dtTrainDataPath, dataPath, name, delimiter, numClasses, modelResultPath, resultPath, impurity, maxDepth, maxBins)
      sender() ! DTTaskResult(modelResultPath, result, resultPath)
    }
  }
}
