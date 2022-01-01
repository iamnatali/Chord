import Node.MyJoin
import akka.actor.{ActorPath, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

object Configs {
  private val root = ConfigFactory.load()
  val firstSystemConfig: Config = root.getConfig("systemOne")
  val secondSystemConfig: Config = root.getConfig("systemTwo")

  val secondSystemHost: String =
    secondSystemConfig.getString(
      "akka.remote.artery.canonical.hostname"
    ) //мб exception
  val secondSystemPort: String =
    secondSystemConfig.getString(
      "akka.remote.artery.canonical.port"
    ) //мб exception

  val existingNodePath: ActorPath =
    ActorPath.fromString(
      secondSystemConfig.getString("existingNodePath")
    ) //мб exception
}

object Main {
  import Configs._

  def main(args: Array[String]): Unit = {
    val existingSystem = ActorSystem("systemOne", firstSystemConfig)
    val existingNode = existingSystem.actorOf(Props[Node], "existingNode")
    println(s"first local path ${existingNode.path}")

    val newSystem = ActorSystem("systemTwo", secondSystemConfig)
    val newNode = newSystem.actorOf(Props[Node], "newNode")

    println(s"second local path ${newNode.path}")

    newNode ! MyJoin(existingNodePath, s"$secondSystemHost:$secondSystemPort")
  }
}
