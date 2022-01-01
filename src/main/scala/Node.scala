import Node.{MyJoin, MyLeave, OthersJoin, OthersLeave, sha1}
import Resolver.{NotResolved, Resolved}
import akka.actor._

import scala.language.postfixOps

class Node extends Actor with ActorLogging {
  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 5) {
      case _: Exception => SupervisorStrategy.restart
    }

  def internalReceive: Receive = {
    //меня создали и просят сделать все, что нужно для присоединения
    case j @ MyJoin(existingNodePath, myIpPort) =>
      log.debug(s"Node received $j")
      val resolver = context.actorOf(Props[Resolver], "resolver")
      resolver ! Resolver.Resolve(existingNodePath, myIpPort)

    case r @ Resolved(ref, ipPort) =>
      log.debug(s"Node received $r")
      ref ! OthersJoin(ipPort)
    case r @ NotResolved(_) =>
      log.debug(s"Node received $r")

    //другой Node хочет присоединиться ко мне
    case oth @ OthersJoin(joinCandidateIp) =>
      context.watchWith(sender, OthersLeave(sender))
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
    //Node, который подсоединялся с помощью нас, ушел
    //Under the hood remote death watch uses heartbeat messages and a
    //failure detector to generate Terminated message from network failures and JVM crashes,
    //in addition to graceful termination of watched actor.
    case oth @ OthersLeave(_) =>
      log.debug(s"Node received $oth")
    case ml @ MyLeave =>
      log.debug(s"Node received $ml")
      context.stop(self)
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

  case class OthersLeave(leavingNode: ActorRef) extends JsonSerializable
  case object MyLeave extends JsonSerializable
  case class OthersJoin(ipPort: String)
      extends JsonSerializable //пока что ip + port
  case class MyJoin(`existingNodePath`: ActorPath, `myIpPort`: String)
      extends JsonSerializable
}
