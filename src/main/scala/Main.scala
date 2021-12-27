import Node.MyJoin
import akka.actor.{ActorPath, ActorSystem, Props}
import cats.effect.{IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}

class Ip(val value: String) extends AnyVal

//заменить config factory на tofu config(?)

object Configs {
  private val root = IO.delay(ConfigFactory.load())
  val firstSystemConfigIO: IO[Config] = root.map(_.getConfig("systemOne"))
  val secondSystemConfigIO: IO[Config] = root.map(_.getConfig("systemOne"))

  val secondSystemHostIO: IO[String] = {
    secondSystemConfigIO.map(
      _.getString(
        "akka.remote.artery.canonical.hostname"
      )
    )
  } //exception
  val secondSystemPortIO: IO[String] =
    secondSystemConfigIO.map(
      _.getString(
        "akka.remote.artery.canonical.port"
      )
    ) //exception

  val existingNodePathIO: IO[ActorPath] = {
    secondSystemConfigIO.map(c =>
      ActorPath.fromString(
        c.getString("existingNodePath")
      )
    )
  } //exception
}

//переписать на ФП
object Main extends IOApp.Simple {
  import Configs._

  val run = for {
    firstSystemConfig <- firstSystemConfigIO
    existingSystem = ActorSystem("systemOne", firstSystemConfig)
    existingNode = existingSystem.actorOf(Props[Node], "existingNode")
    _ <- IO.println(s"first local path ${existingNode.path}")
    secondSystemConfig <- secondSystemConfigIO
    newSystem = ActorSystem("systemTwo", secondSystemConfig)
    newNode = newSystem.actorOf(Props[Node], "newNode")
    _ <- IO.println(s"second local path ${newNode.path}")
    existingNodePath <- existingNodePathIO
  } yield newNode ! MyJoin(
    existingNodePath,
    s"$secondSystemHostIO:$secondSystemPortIO"
  )

//  val existingSystem = ActorSystem("systemOne", firstSystemConfigIO) //
//  val existingNode = existingSystem.actorOf(Props[Node], "existingNode") //
//  println(s"first local path ${existingNode.path}") //
//
//  //We assume that the new node learns the identity of an existing Chord node by some external mechanism.
//  //Наверное нужна функция - по имени системы получаем existingNode remote path
//  //akka://systemOne/user/node
//  //akka://systemOne@127.0.0.1:2552/user/node
//
//  val newSystem = ActorSystem("systemTwo", secondSystemConfigIO) //
//  val newNode = existingSystem.actorOf(Props[Node], "newNode") //
//
//  println(s"second local path ${newNode.path}")
//
//  //sha1 test
////  val r1 = Node.sha1("127.0.0.1:2553")
////  val r2 = Node.sha1("100.0.0.0:0000")
////
////  println("sha test")
////  println(r1)
////  println(r1.bitLength)
////  println(r2)
////  println(r2.bitLength)
////  println("sha test end")
//
//  newNode ! MyJoin(
//    existingNodePathIO,
//    s"$secondSystemHostIO:$secondSystemPortIO"
//  )
}
