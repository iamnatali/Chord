import Node._
import Resolver.{NotResolved, Resolved}
import akka.actor._
import akka.event.LoggingReceive
import akka.actor.Timers

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.postfixOps
import scala.util.Random

case class Node(
    myId: BigInt,
    m: Int,
    var fingerTable: Map[BigInt, NodeInfo],
    var predecessor: Option[NodeInfo],
    startBehavior: String = "initializing",
    stabilizeTimeout: FiniteDuration = 5.seconds,
    fixFingersTimeout: FiniteDuration = 5.seconds,
    responseTimeout: FiniteDuration = 5.seconds
) extends Actor
    with ActorLogging
    with Timers {

  override val supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 5) {
      case _: Exception => SupervisorStrategy.restart
    }

  val selfNodeInfo: NodeInfo = NodeInfo(self, myId)

  //пока что считаю что все сообщения всегда доходят
  //не хотим хранить состояние актора и всегда заново подсоединяемся

  def initializing: Receive =
    LoggingReceive {
      case StartCircle =>
        log.debug(s"myNode $selfNodeInfo")
        fingerTable =
          List.tabulate(m)(i => (myId + BigInt(2).pow(i)) -> selfNodeInfo).toMap
        predecessor = Some(selfNodeInfo)
        context.become(internalReceive)
        timers.startTimerWithFixedDelay(
          StabilizeKey,
          Stabilize(1),
          0.seconds,
          stabilizeTimeout
        )
        timers.startTimerAtFixedRate(
          FixFingersKey,
          FixFingers,
          fixFingersTimeout
        )
        timers.startTimerWithFixedDelay(
          PrintTableKey,
          PrintFingerTable,
          0.seconds,
          10.seconds
        )

      case MyJoin(existingNodePath) =>
        log.debug(s"myNode $selfNodeInfo")
        val resolver = context.actorOf(Props[Resolver], "resolver")
        resolver ! Resolver.Resolve(existingNodePath)

      case Resolved(nsh) =>
        nsh ! FindPredecessor(
          fingerStart(myId, 1, m),
          self,
          None,
          None
        )
        timers.startSingleTimer(
          InitializingWaitsForFoundPredecessor,
          Dead(nsh, InitializingWaitsForFoundPredecessor),
          responseTimeout
        )

      case Dead(_, InitializingWaitsForFoundPredecessor) => context.stop(self)

      case FoundPredecessor(
            _,
            queryPredecessor,
            predecessorSuccessor,
            _,
            _
          ) =>
        timers.cancel(InitializingWaitsForFoundPredecessor)
        predecessor = Some(queryPredecessor)
        fingerTable =
          fingerTable.updated(fingerStart(myId, 1, m), predecessorSuccessor)
        context.become(internalReceive)
        timers.startTimerWithFixedDelay(
          StabilizeKey,
          Stabilize(1),
          0.seconds,
          stabilizeTimeout
        )
        timers.startTimerAtFixedRate(
          FixFingersKey,
          FixFingers,
          fixFingersTimeout
        )
        timers.startTimerWithFixedDelay(
          s"PrintTableKey $myId",
          PrintFingerTable,
          0.seconds,
          10.seconds
        )
    }

  def stabilize: Receive = {
    case Stabilize(k) =>
      if (k <= m) {
        val successor = fingerTable(fingerStart(myId, k, m))
        successor.ref ! GetPredecessor("stabilize")
        timers.startSingleTimer(
          StabilizeWaitsForGotPredecessor,
          Dead(successor.ref, (StabilizeWaitsForGotPredecessor, k)),
          responseTimeout
        )
      } else
        throw new Exception(
          s"no alive nodes in finger table of node: $selfNodeInfo"
        )
    case Dead(_, (StabilizeWaitsForGotPredecessor, k: Int)) =>
      self ! Stabilize(k + 1)
    case GotPredecessor(succPredcessor, "stabilize") =>
      timers.cancel(StabilizeWaitsForGotPredecessor)
      val successor = fingerTable(fingerStart(myId, 1, m))
      succPredcessor.foreach(x =>
        if (belongsClockwise00(x.circleId, myId, successor.circleId, m)) {
          fingerTable = fingerTable.updated(fingerStart(myId, 1, m), x)
        }
      )
      fingerTable(fingerStart(myId, 1, m)).ref ! Notify(selfNodeInfo)
  }

  def fixFingers: Receive = {
    case FixFingers =>
      val i = Random.nextInt(m) + 1
      val ftKey = fingerStart(myId, i, m)
      log.debug(s"fix fingers start for i: $i key: $ftKey")
      self ! FindSuccessor(ftKey, self, (ftKey, "fixFingers"))

      timers.startSingleTimer(
        FixFingersWaitsForSuccessor,
        NoSuccessorFoundResponse,
        responseTimeout
      )
    case NoSuccessorFoundResponse =>
      throw new Exception(
        s"no successor during fix fingers for node $selfNodeInfo"
      )
    case Successor(successor, _, (ftKey: BigInt, "fixFingers")) =>
      timers.cancel(FixFingersWaitsForSuccessor)
      fingerTable = fingerTable.updated(ftKey, successor)
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
    LoggingReceive(stabilize orElse fixFingers orElse {
      case UpdateFingerTableCertain(newFingerTable) =>
        fingerTable = newFingerTable
        sender ! FingerTableCertainUpdateSuccessful(self)
//      case ChangePredecessor(nodeInfo) =>
//        predecessor = Some(nodeInfo)
//        sender ! PredecessorChangedSuccessfully
      case PrintFingerTable =>
        log.debug(
          s"\nPRINT MY TABLE \n ${Node.fingerTableToString(fingerTable)}"
        )

      case Notify(nsh) =>
        predecessor.foreach(value =>
          if (belongsClockwise00(nsh.circleId, value.circleId, myId, m)) {
            predecessor = Some(nsh)
          }
        )

      case GetPredecessor(info) =>
        sender ! GotPredecessor(predecessor, info)

      case FindSuccessor(id, asker, additionalInfo) =>
        self ! FindPredecessor(id, self, Some(asker), additionalInfo)

      case f @ FindPredecessor(id, asker, successorAsker, additionalInfo) =>
//        println("HERE IT IS 2")
//        println(s"id $id")
//        println(s"myId $myId")
//        println(s"fingerStart(myId, 1, m) ${fingerStart(myId, 1, m)}")
        if (
          Node.belongsClockwise01(
            id,
            myId,
            fingerTable(fingerStart(myId, 1, m)).circleId,
            m
          )
        ) {
//          println("HERE IT IS 3")
//          println(s"asker $asker")
          asker ! FoundPredecessor(
            id,
            selfNodeInfo,
            fingerTable(fingerStart(myId, 1, m)),
            successorAsker,
            additionalInfo
          )
        } else {
//          println("HERE IT IS 4")
//          println(s"info $info")
          val info = closestPrecedingFinger(id, m)
          info.ref ! f
        }

      case FoundPredecessor(
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

      case MyLeave =>
        context.stop(self)
    })

  def receive: Receive =
    if (startBehavior == "initializing")
      initializing
    else internalReceive
}

object Node {
  def apply(ip: String, port: Int, m: Int): Node = {
    Node(sha1(s"$ip:$port", m), m, Map.empty[BigInt, NodeInfo], None)
  }

  //m должно быть кратно 8(или нет?)
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
  case class FoundPredecessor(
      queryId: BigInt,
      predecessor: NodeInfo,
      predecessorSuccessor: NodeInfo,
      asker: Option[ActorRef],
      additionalInfo: Any
  ) extends JsonSerializable
  case class Stabilize(k: Int) extends JsonSerializable
  case class Notify(maybePredecessor: NodeInfo) extends JsonSerializable
  case class GetPredecessor(additionalInfo: Any) extends JsonSerializable
  case class GotPredecessor(predecessor: Option[NodeInfo], additionalInfo: Any)
      extends JsonSerializable
  case object FixFingers extends JsonSerializable
  case object StabilizeKey
  case object FixFingersKey
  case object PrintTableKey
  case class Dead(node: ActorRef, additionalInfo: Any) extends JsonSerializable
  case object InitializingWaitsForFoundPredecessor
  case object StabilizeWaitsForGotPredecessor
  case object FixFingersWaitsForSuccessor
  case object NoSuccessorFoundResponse extends JsonSerializable

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
