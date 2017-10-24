package actors

import akka.actor.Actor
import models.RandomForest

class RFActor extends Actor {
  override def receive = {
    case RFTask(masterHost, masterPort, rfTrainData,predictData, modelResult,predictResult,
    numClasses,numTrees ,name,featureSubsetStrategy, impurity, maxDepth,maxBins ,delimiter) => {
      sender() ! "收到随机森林任务"
      val result = RandomForest.randomForest(rfTrainData, predictData, name,featureSubsetStrategy, delimiter, numClasses,numTrees, modelResult, predictResult, impurity, maxDepth, maxBins)
      sender() ! RFTaskResult(modelResult,result.toString,predictResult)
    }
  }
}
