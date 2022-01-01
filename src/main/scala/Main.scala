import Node.{MyJoin, MyLeave}
import akka.actor.{ActorPath, ActorSystem, Props}
import com.sun.javaws.exceptions.InvalidArgumentException
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.jdk.CollectionConverters.MapHasAsJava

object Configs {
  val systemConfig: Config = ConfigFactory.load()

  def configureHostPort(prev: Config, host: String, port: Int): Config =
    prev
      .withValue(
        "akka.remote.artery.canonical",
        ConfigValueFactory.fromMap(
          Map("hostname" -> host, "port" -> port).asJava
        )
      )
}

object Main {
  import Configs._

  //нужно записать схему взаимодействия
  //принимаем от 2х до 3х аргументов
  //хост, порт и адрес того, к кому подключаемся
  //чисто в теории keepalive и terminated должны отслеживать successor, но пока что тот, к которому присоединились
  def main(args: Array[String]): Unit = {
    args.toList match {
      case host :: port :: tail =>
        val hostPortSystemConfig =
          Configs.configureHostPort(systemConfig, host, port.toInt)
        tail match {
          case existingNodePathString :: Nil =>
            val newSystem = ActorSystem("chordNodeSystem", hostPortSystemConfig)
            val newNode = newSystem.actorOf(Props[Node])
            val existingNodePath = ActorPath.fromString(existingNodePathString)
            newNode ! MyJoin(existingNodePath, s"$host:$port")
//            Thread.sleep(10000)
//            newNode ! MyLeave
          case Nil =>
            val existingSystem =
              ActorSystem("chordNodeSystem", hostPortSystemConfig)
            existingSystem.actorOf(Props[Node], "existingNode")
          case _ =>
            throw new InvalidArgumentException(
              Array[String]("invalid command line arguments")
            )
        }
      case _ =>
        throw new InvalidArgumentException(
          Array[String]("invalid command line arguments")
        )
    }
  }
}
