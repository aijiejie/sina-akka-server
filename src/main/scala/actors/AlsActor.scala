package actors

import akka.actor.Actor
import models.Als

class AlsActor extends Actor{
  override def receive = {
    case AlsTask(masterHost, masterPort, datapath, dataResultPath, alsRseultNumber, name, rank, iter, delimiter) =>{
      val result = Als.als(datapath, name, dataResultPath, alsRseultNumber, rank, iter, delimiter)
      if (result) sender() ! "ALS推荐算法任务成功结束"
    }
  }
}

