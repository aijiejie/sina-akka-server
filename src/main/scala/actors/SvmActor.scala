package actors

import akka.actor.Actor
import models.SVM

class SvmActor extends Actor {
  override def receive = {
    case SvmTask(svmMasterHost, svmMasterPort, svmTrainDataPath,
    svmPredictDataPath, svmModelResultPath, svmPredictResultPath,
    name, iter) => {
      sender() ! "收到SVM任务"
      val result = SVM.svm(svmMasterHost, svmMasterPort, svmTrainDataPath,
        svmPredictDataPath, svmModelResultPath, svmPredictResultPath,
        name, iter) //用akka实现
      sender() ! SvmTaskResult(result, svmModelResultPath, svmPredictResultPath)
    }
  }
}