import Node._
import Resolver.{NotResolved, Resolved}
import akka.actor._

import scala.language.postfixOps

case class Node(ip: String, port: Int, m: Int) extends Actor with ActorLogging {
  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 5) {
      case _: Exception => SupervisorStrategy.restart
    }

  var fingerTable = Map.empty[Int, NodeInfo]
  var predecessor: Option[NodeInfo] = None //убрать option через behavior

  var ipPort = s"$ip:$port"
  var myId: BigInt = sha1(s"$ip:$port", m)

  println(ipPort)
  println(s"id $myId")

  //пока что считаю что все сообщения всегда доходят
  //хотим хранить состояние актора или всегда заново подсоединяемся?

  def initializing: Receive = {
    case sc @ StartCircle =>
      log.debug(s"Node received $sc")
      val n = NodeInfo(self, myId)
      fingerTable = List.tabulate(m)(i => i + 1 -> n).toMap
      predecessor = Some(n)
      context.become(internalReceive)
      self ! PrintFingerTable

    case j @ MyJoin(existingNodePath) =>
      log.debug(s"Node received $j")
      val resolver = context.actorOf(Props[Resolver], "resolver")
      resolver ! Resolver.Resolve(existingNodePath)
    case r @ Resolved(ref) =>
      log.debug(s"Node received $r")
      ref ! FindSuccessor(
        fingerStart(myId, 1, m),
        self,
        1
      ) //номер i для finger[i].node = ref.findSuccessor(finger[i].start)
      context.become(initFingerTable(ref, 0))

    case r @ NotResolved =>
      log.debug(s"Node received $r")
    case anyOther =>
      log.debug(s"Node received unknown $anyOther on behavor initializing")
  }

  def initFingerTable(ref: ActorRef, sentFindSuccessorCount: Int): Receive = {
    case s @ Successor(successor, successorPredecessor, 1) =>
      log.debug(s"Node received $s")
      fingerTable = fingerTable.updated(1, successor)
      predecessor = Some(successorPredecessor)
      successor.ref ! ChangePredecessor(NodeInfo(self, myId))
    case s @ PredecessorChangedSuccessfully =>
      log.debug(s"Node received $s")
      for (i <- 1 until m) {
        val fs = fingerStart(myId, i + 1, m)
        val fti = fingerTable(i)
        if (fs <= fti.circleId && fs >= myId) { //создать для этого по актору?
          fingerTable = fingerTable.updated(i + 1, fti)
        } else {
          ref ! FindSuccessor(fingerStart(myId, i + 1, m), self, i + 1)
          context.become(initFingerTable(ref, sentFindSuccessorCount + 1))
        }
      }
    case s @ Successor(successor, _, iPlus: Int) =>
      log.debug(s"Node received $s")
      if (iPlus > 1) {
        fingerTable = fingerTable.updated(iPlus, successor)
        context.become(initFingerTable(ref, sentFindSuccessorCount + 1))
      } else {
        fingerTable = fingerTable.updated(iPlus, successor)
        context.become(internalReceive)
        println("we went to the internalReceive")
        self ! PrintFingerTable
      }
    case anyOther =>
      log.debug(s"Node received unknown $anyOther on behavor internal receive")
  }

  def internalReceive: Receive = {
    case oth @ OthersLeave(_) =>
      log.debug(s"Node received $oth")
    case ml @ MyLeave =>
      log.debug(s"Node received $ml")
      context.stop(self)

    case s @ ChangePredecessor(nodeInfo) =>
      log.debug(s"Node received $s")
      predecessor = Some(nodeInfo)
      sender ! PredecessorChangedSuccessfully

    case s @ FindSuccessor(id, asker, additionalInfo) =>
      log.debug(s"Node received $s")
      self ! FindPredecessor(id, self, Some(asker), additionalInfo)

    // не замкнут круг
    case f @ FindPredecessor(id, asker, successorAsker, additionalInfo) =>
      //log.debug(s"Node received $f")
      log.debug(s"id $id")
      log.debug(s"myid $myId")
      log.debug(s"successor ${fingerTable(1).circleId}")
      //id <= fingerTable(1).circleId && id > myId
      if (true) {
        asker ! Predecessor(
          id,
          NodeInfo(self, myId),
          fingerTable(1),
          successorAsker,
          additionalInfo
        )
      } else {
        val info = Node.closestPrecedingFinger(this, id, m)
        info.ref ! f
      }

    case p @ Predecessor(
          _,
          predecessor,
          predecessorSuccessor,
          asker,
          additionalInfo
        ) =>
      log.debug(s"Node received $p")
      asker match {
        case Some(ask) =>
          ask ! Successor(predecessorSuccessor, predecessor, additionalInfo)
        case None => log.debug(s"got predecessor $predecessor")
      }

    case s @ Successor(_, _, _) =>
      log.debug(s"Node received $s")

    case PrintFingerTable =>
    case anyOther =>
      log.debug(s"Node received unknown $anyOther on behavor internal receive")
  }

  def receive: Receive = initializing
}

object Node {
  def sha1(s: String, m: Int): BigInt = {
    val ar = java.security.MessageDigest
      .getInstance("SHA-1")
      .digest(s.getBytes("UTF-8"))
      .toList :+ 0.toByte
    BigInt(ar.toArray)
  }

  def fingerStart(n: BigInt, k: Int, m: Int): BigInt =
    (n + BigInt(2) pow (k - 1)) mod (BigInt(2) pow m)

  def fingerInterval(n: BigInt, k: Int, m: Int): (BigInt, BigInt) =
    (
      fingerStart(n, k, m),
      fingerStart(n, k - 1, m)
    ) //включительно, невключительно

  def belongs(
      id: BigInt,
      intervalStart: BigInt,
      intervalEnd: BigInt,
      m: Int
  ): Boolean = true

  def closestPrecedingFinger(n: Node, id: BigInt, m: Int): NodeInfo =
    List
      .range(1, m + 1)
      .reverse
      .map(i => n.fingerTable(i))
      .find(nodeInfo => belongs(nodeInfo.circleId, n.myId, id, 1)) match {
      case Some(value) => value
      case None        => NodeInfo(n.self, n.myId)
    }

  case class NodeInfo(
      ref: ActorRef,
      circleId: BigInt
  )

  case object PrintFingerTable extends JsonSerializable

  case object StartCircle extends JsonSerializable

  case class OthersLeave(leavingNode: ActorRef) extends JsonSerializable
  case object MyLeave extends JsonSerializable

  case class OthersJoin(ipPort: String) extends JsonSerializable //ip:port
  case class MyJoin(`existingNodePath`: ActorPath) extends JsonSerializable

  case class FindSuccessor(id: BigInt, asker: ActorRef, additionalInfo: Any)
      extends JsonSerializable
  case class Successor(
      nodeInfo: NodeInfo,
      successorPredecessor: NodeInfo,
      additionalInfo: Any
  ) extends JsonSerializable

  case class FindPredecessor(
      id: BigInt,
      asker: ActorRef,
      successorAsker: Option[ActorRef],
      additionalInfo: Any
  ) extends JsonSerializable
  case class Predecessor(
      queryId: BigInt,
      predecessor: NodeInfo,
      predecessorSuccessor: NodeInfo,
      asker: Option[ActorRef],
      additionalInfo: Any
  ) extends JsonSerializable

  case class ChangePredecessor(nodeInfo: NodeInfo) extends JsonSerializable

  case object PredecessorChangedSuccessfully extends JsonSerializable
}

object ShaTest extends App {
  println(Node.sha1("a", 10)) //1970026582
  println(Node.sha1("00000", 10)) //953377235
}
