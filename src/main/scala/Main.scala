import Node.MyJoin
import akka.actor.{ActorPath, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

class Ip(val value: String) extends AnyVal

//заменить config factory на tofu config(?)

object Configs {
  private val root = ConfigFactory.load()
  val firstSystemConfig: Config = root.getConfig("systemOne")
  val secondSystemConfig: Config = root.getConfig("systemTwo")

  val secondSystemHost: String =
    secondSystemConfig.getString(
      "akka.remote.artery.canonical.hostname"
    ) //exception
  val secondSystemPort: String =
    secondSystemConfig.getString(
      "akka.remote.artery.canonical.port"
    ) //exception

  val existingNodePath: ActorPath =
    ActorPath.fromString(
      secondSystemConfig.getString("existingNodePath")
    ) //exception
}

//переписать на ФП
object Main extends App {
  import Configs._

  val existingSystem = ActorSystem("systemOne", firstSystemConfig)
  val existingNode = existingSystem.actorOf(Props[Node], "existingNode")
  println(s"first local path ${existingNode.path}")

  //We assume that the new node learns the identity of an existing Chord node by some external mechanism.
  //Наверное нужна функция - по имени системы получаем existingNode remote path
  //akka://systemOne/user/node
  //akka://systemOne@127.0.0.1:2552/user/node

  val newSystem = ActorSystem("systemTwo", secondSystemConfig)
  val newNode = existingSystem.actorOf(Props[Node], "newNode")

  println(s"second local path ${newNode.path}")

  //sha1 test
  val r1 = Node.sha1("127.0.0.1:2553")
  val r2 = Node.sha1("100.0.0.0:0000")

  println("sha test")
  println(r1)
  println(r1.bitLength) //
  println(r2)
  println(r2.bitLength)
  println("sha test end")

  newNode ! MyJoin(existingNodePath, s"$secondSystemHost:$secondSystemPort")
}
