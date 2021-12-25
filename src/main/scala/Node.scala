import Node.{MyJoin, OthersJoin, sha1}
import Resolver.{NotResolved, Resolved}
import akka.actor.{Actor, ActorLogging, ActorPath, Props}

import scala.language.postfixOps

//https://stackoverflow.com/questions/14288068/how-do-i-get-the-absolute-remote-actor-url-from-inside-the-actor
//netstat -anp | grep $PORT
//https://doc.akka.io/docs/akka/current/remoting-artery.html#selecting-a-transport
//https://www.coursera.org/learn/scala-akka-reactive/lecture/mVLWq/lecture-4-1-actors-are-distributed-part-1

class Node extends Actor with ActorLogging {
  //override val supervisorStrategy = подумать над обработкой ошибок

  def internalReceive: Receive = {
    //меня создали и просят сделать все, что нужно для присоединения
    case j @ MyJoin(existingNodePath, myIpPort) =>
      log.debug(s"Node received $j")
      val resolver = context.actorOf(Props[Resolver], "resolver")
      resolver ! Resolver.Resolve(existingNodePath, myIpPort)

    case r @ Resolved(_, ref, ipPort) =>
      log.debug(s"Node received $r")
      ref ! OthersJoin(ipPort)
    case r @ NotResolved(_, _) =>
      log.debug(s"Node received $r")

    //другой Node хочет присоединиться ко мне
    case oth @ OthersJoin(joinCandidateIp) =>
      log.debug(s"Node received $oth")
      val id = sha1(joinCandidateIp)
      //let m be the number of bits in the key/node identifiers
      //finger table of node n
      //Map[i-int:s-(ActorRef, circleId)]
      //s = successor(n + 2^(i-1)) 1 <=i <= m
      //s <=> n.finger[i].node
      log.debug(
        s"Node with ipPort: $joinCandidateIp id: $id joined"
      )
  }

  def receive: Receive = internalReceive
}

object Node {
  def sha1(s: String): BigInt = {
    val ar = java.security.MessageDigest
      .getInstance("SHA-1")
      .digest(s.getBytes)
    BigInt(ar)
  }

  case class OthersJoin(ipPort: String) //пока что ip + port
  case class MyJoin(existingNodePath: ActorPath, myIpPort: String)
}
