import Node._
import Resolver.{NotResolved, Resolved}
import akka.actor._
import akka.event.LoggingReceive

import scala.language.postfixOps

case class Node(
    myId: BigInt,
    m: Int,
    var fingerTable: Map[BigInt, NodeInfo],
    var predecessor: Option[NodeInfo],
    startBehavior: String = "internalReceive",
    notifyRef: Option[ActorRef] = None
) extends Actor
    with ActorLogging {

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 5) {
      case _: Exception => SupervisorStrategy.restart
    }

  val selfNodeInfo: NodeInfo = NodeInfo(self, myId)

  //пока что считаю что все сообщения всегда доходят
  //хотим хранить состояние актора или всегда заново подсоединяемся?

  def initializing: Receive =
    LoggingReceive {
      case StartCircle =>
        fingerTable =
          List.tabulate(m)(i => (myId + BigInt(2).pow(i)) -> selfNodeInfo).toMap
        predecessor = Some(selfNodeInfo)
        context.become(internalReceive)
        self ! PrintFingerTable

      case MyJoin(existingNodePath) =>
        val resolver = context.actorOf(Props[Resolver], "resolver")
        resolver ! Resolver.Resolve(existingNodePath)
      case Resolved(nsh) =>
        nsh ! FindSuccessor(
          fingerStart(myId, 1, m),
          self,
          0
        ) //номер i-1 для finger[i].node = ref.findSuccessor(finger[i].start)
        context.become(initFingerTable(nsh))

      case NotResolved =>
    }

  //The DeathWatch service is idempotent, meaning that registering twice has the same effect as registering once.
  def updateFingerTable(i: BigInt, nodeInfo: NodeInfo) = {
    fingerTable = fingerTable.updated(i, nodeInfo)
//    if (nodeInfo.circleId != myId) {
//      context.watchWith(nodeInfo.ref, OthersLeave(nodeInfo.ref))
//    }
  }

  def initFingerTable(nsh: ActorRef): Receive =
    LoggingReceive {
      case Successor(successor, successorPredecessor, 0) =>
        updateFingerTable(fingerStart(myId, 1, m), successor)
        predecessor = Some(successorPredecessor)
        successor.ref ! ChangePredecessor(selfNodeInfo)
      case PredecessorChangedSuccessfully =>
        self ! InitTableCycle(1)
      case InitTableCycle(i: Int) =>
        if (i < m) {
          val fsi1 = fingerStart(myId, i + 1, m)
          val fsi = fingerStart(myId, i, m)
          val fti = fingerTable(fsi)
          if (Node.belongsClockwise10(fsi1, myId, fti.circleId, m)) {
            updateFingerTable(fsi1, fti)
            log.debug(s"finger table updated on key ${i + 1} with $fti")
            self ! InitTableCycle(i + 1)
          } else {
            nsh ! FindSuccessor(fingerStart(myId, i + 1, m), self, i)
          }
        } else {
          notifyRef match {
            case Some(r) =>
              r ! TableInitiated
              context.become(getFingerTable)
            case None =>
              context.become(updateOthers)
              self ! UpdateTableCycle(1)
              log.debug("changed behavior to updateOthers")
          }
        }

      case Successor(successor, _, i: Int) =>
        val fsi1 = fingerStart(myId, i + 1, m)
        updateFingerTable(fsi1, successor)
        self ! InitTableCycle(i + 1)
    }

  def getFingerTable: Receive = {
    case GetFingerTable => sender ! FingerTable(fingerTable)
  }

  def updateOthers: Receive =
    LoggingReceive(
      getFingerTable
        .orElse(updateFingerTableReceive)
        .orElse({
          case UpdateFingerTableCertain(newFingerTable) =>
            fingerTable = newFingerTable
            sender ! FingerTableCertainUpdateSuccessful(self)
          case UpdateTableCycle(i: Int) =>
            self ! FindPredecessor(
              (myId - BigInt(2).pow(i - 1)).mod(BigInt(2).pow(m)),
              self,
              None,
              i
            )
          case Predecessor(_, predecessor, _, _, i: Int) =>
            predecessor.ref ! UpdateFingerTable(selfNodeInfo, i, self)
          case FingerTableUpdateEnded(i: Int) =>
            if (i < m) self ! UpdateTableCycle(i + 1)
            else {
              notifyRef.foreach(r => r ! OthersTablesUpdated)
              context.become(internalReceive)
              log.debug("changed behavior to internalReceive")
              self ! PrintFingerTable
            }
          case FindSuccessor(id, asker, additionalInfo) =>
            self ! FindPredecessor(id, self, Some(asker), additionalInfo)

          case f @ FindPredecessor(id, asker, successorAsker, additionalInfo) =>
            if (
              Node.belongsClockwise01(
                id,
                myId,
                fingerTable(fingerStart(myId, 1, m)).circleId,
                m
              )
            ) {
              println("HERE IT IS 3")
              println(s"asker $asker")
              asker ! Predecessor(
                id,
                selfNodeInfo,
                fingerTable(fingerStart(myId, 1, m)),
                successorAsker,
                additionalInfo
              )
            } else {
              println("HERE IT IS 4")
              val info = closestPrecedingFinger(id, m)
              println(s"info $info")
              info.ref ! f
            }

          case PrintFingerTable =>
            log.debug(Node.fingerTableToString(fingerTable))
        })
    )

  def updateFingerTableReceive: Receive =
    LoggingReceive {
      case upd @ UpdateFingerTable(s, i, asker) =>
        log.debug(s"s.circleId ${s.circleId}")
        log.debug(s"myId $myId")
        if (
          s.circleId != fingerTable(fingerStart(myId, i, m)).circleId &&
          Node.belongsClockwise10(
            s.circleId,
            myId,
            fingerTable(fingerStart(myId, i, m)).circleId,
            m
          )
        ) {
          updateFingerTable(i, s)
          predecessor match {
            case Some(pred) =>
              if (pred.ref != asker)
                pred.ref ! upd
            case None =>
              log.error(s"predecessor is needed but is not available")
          }
          asker ! FingerTableUpdateEnded(i)
        } else asker ! FingerTableUpdateEnded(i)
    }

  def closestPrecedingFinger(id: BigInt, m: Int): NodeInfo =
    List
      .range(1, m)
      .reverse
      .map(i => fingerTable.get(myId + BigInt(2).pow(i - 1)))
      .collect({ case Some(finger) => finger })
      .find(nodeInfo =>
        belongsClockwise00(nodeInfo.circleId, myId, id, m)
      ) match {
      case Some(value) => value
      case None        => fingerTable(myId + BigInt(2).pow(m - 1))
    }

  def internalReceive: Receive =
    LoggingReceive(updateFingerTableReceive orElse {
      case UpdateFingerTableCertain(newFingerTable) =>
        fingerTable = newFingerTable
        sender ! FingerTableCertainUpdateSuccessful(self)

      case OthersLeave(_) =>
      case MyLeave =>
        context.stop(self)

      case ChangePredecessor(nodeInfo) =>
        predecessor = Some(nodeInfo)
        sender ! PredecessorChangedSuccessfully

      case FindSuccessor(id, asker, additionalInfo) =>
        self ! FindPredecessor(id, self, Some(asker), additionalInfo)

      case f @ FindPredecessor(id, asker, successorAsker, additionalInfo) =>
        println("HERE IT IS 2")
        println(s"id $id")
        println(s"myId $myId")
        println(s"fingerStart(myId, 1, m) ${fingerStart(myId, 1, m)}")
        if (
          Node.belongsClockwise01(
            id,
            myId,
            fingerTable(fingerStart(myId, 1, m)).circleId,
            m
          )
        ) {
          println("HERE IT IS 3")
          println(s"asker $asker")
          asker ! Predecessor(
            id,
            selfNodeInfo,
            fingerTable(fingerStart(myId, 1, m)),
            successorAsker,
            additionalInfo
          )
        } else {
          println("HERE IT IS 4")
          val info = closestPrecedingFinger(id, m)
          println(s"info $info")
          info.ref ! f
        }

      case Predecessor(
            _,
            predecessor,
            predecessorSuccessor,
            asker,
            additionalInfo
          ) =>
        asker match {
          case Some(ask) =>
            ask ! Successor(predecessorSuccessor, predecessor, additionalInfo)
          case None =>
        }

      case Successor(_, _, _) =>
      case PrintFingerTable =>
        log.debug(Node.fingerTableToString(fingerTable))
    })

  def receive: Receive =
    if (startBehavior == "initializing")
      initializing
    else if (startBehavior == "updateOthers")
      updateOthers
    else internalReceive
}

object Node {
  def apply(ip: String, port: Int, m: Int): Node = {
    Node(sha1(s"$ip:$port", m), m, Map.empty[BigInt, NodeInfo], None)
  }

  //m должно быть кратно 8
  def sha1(s: String, m: Int): BigInt = {
    val ar = java.security.MessageDigest
      .getInstance("SHA-1")
      .digest(s.getBytes("UTF-8"))
      .toList :+ 0.toByte //length 20 байт - 160 бит
    val a = ar.take(m / 8).toArray
    BigInt(1, a)
  }

  def fingerTableToString(map: Map[BigInt, NodeInfo]): String = {
    map.map { case (i, nodeInfo) => s"$i | $nodeInfo" }.mkString("\n")
  }

  def fingerStart(n: BigInt, k: Int, m: Int): BigInt =
    (n + BigInt(2).pow(k - 1)).mod(BigInt(2).pow(m))

  def belongsClockwise11(
      id: BigInt,
      intervalStart: BigInt,
      intervalEnd: BigInt,
      m: Int
  ): Boolean = {
    val largest = BigInt(2).pow(m)
    val intervalStartM = intervalStart.mod(largest)
    val intervalEndM = intervalEnd.mod(largest)
    val idM = id.mod(largest)
    if (intervalStartM < intervalEndM)
      intervalStartM <= idM && idM <= intervalEndM
    else
      intervalStartM <= idM || idM <= intervalEndM
  }

  def belongsClockwise10(
      id: BigInt,
      intervalStart: BigInt,
      intervalEnd: BigInt,
      m: Int
  ): Boolean = {
    val largest = BigInt(2).pow(m)
    val intervalStartM = intervalStart.mod(largest)
    val intervalEndM = intervalEnd.mod(largest)
    val idM = id.mod(largest)
    if (intervalStartM < intervalEndM)
      intervalStartM <= idM && idM < intervalEndM
    else
      intervalStartM <= idM || idM < intervalEndM
  }

  def belongsClockwise01(
      id: BigInt,
      intervalStart: BigInt,
      intervalEnd: BigInt,
      m: Int
  ): Boolean = {
    val largest = BigInt(2).pow(m)
    val intervalStartM = intervalStart.mod(largest)
    val intervalEndM = intervalEnd.mod(largest)
    val idM = id.mod(largest)
    if (intervalStartM < intervalEndM)
      intervalStartM < idM && idM <= intervalEndM
    else
      intervalStartM < idM || idM <= intervalEndM
  }

  def belongsClockwise00(
      id: BigInt,
      intervalStart: BigInt,
      intervalEnd: BigInt,
      m: Int
  ): Boolean = {
    val largest = BigInt(2).pow(m)
    val intervalStartM = intervalStart.mod(largest)
    val intervalEndM = intervalEnd.mod(largest)
    val idM = id.mod(largest)
    if (intervalStartM < intervalEndM)
      intervalStartM < idM && idM < intervalEndM
    else
      intervalStartM < idM || idM < intervalEndM
  }

  case class NodeInfo(
      ref: ActorRef,
      circleId: BigInt
  )

  case object GetFingerTable extends JsonSerializable

  case class FingerTable(map: Map[BigInt, NodeInfo]) extends JsonSerializable

  case object TableInitiated extends JsonSerializable

  case object OthersTablesUpdated extends JsonSerializable

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

  case class InitTableCycle(i: Int) extends JsonSerializable

  case class UpdateTableCycle(i: Int) extends JsonSerializable

  case class UpdateFingerTable(nodeInfo: NodeInfo, i: Int, asker: ActorRef)
      extends JsonSerializable

  case class UpdateFingerTableCertain(newFingerTable: Map[BigInt, NodeInfo])
      extends JsonSerializable

  case class FingerTableCertainUpdateSuccessful(ref: ActorRef)
      extends JsonSerializable

  case class FingerTableUpdateEnded(i: Int) extends JsonSerializable
}

object ShaTest extends App {
  println(Node.sha1("a", 10)) //1970026582
  println(Node.sha1("00000", 10)) //953377235
}
