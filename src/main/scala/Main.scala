import actors.SeverActor.serverActor
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}

import scala.concurrent.duration._
import scala.concurrent.Await

object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {

    val host = args(0)
    val port = args(1)
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem("MasterActor", config)
    //val supervisor = actorSystem.actorOf(Props[Supervisor],"Supervisor")//监控server测试
    //supervisor ! Props(new actors.SeverActor())

    serverActor = actorSystem.actorOf(Props(new actors.SeverActor()), "Server")
    Await.ready(actorSystem.whenTerminated, 30.minutes)
  }
}
