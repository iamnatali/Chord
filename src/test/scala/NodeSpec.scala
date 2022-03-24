import Node.{
  ChangePredecessor,
  FindPredecessor,
  FindSuccessor,
  FingerTable,
  FingerTableCertainUpdateSuccessful,
  FoundPredecessor,
  GetFingerTable,
  GetPredecessor,
  GotPredecessor,
  NodeInfo,
  Notify,
  OthersTablesUpdated,
  PredecessorChangedSuccessfully,
  StabilizeKey,
  Successor,
  TableInitiated,
  UpdateFingerTableCertain,
  UpdateTableCycle
}
import Resolver.Resolved
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.TestProbe
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration, MINUTES}

class NodeSpec extends AnyWordSpecLike with Matchers {

  "Node Actor" must {

    implicit val system: ActorSystem = ActorSystem()

    "init finger tables correctly" in {
      val p = TestProbe("testProbe")

      val m = 3

      val internal = "internalReceive"

      val n0: ActorRef =
        system.actorOf(Props(Node(0, m, Map.empty, None, internal)), "n0")
      val n1: ActorRef =
        system.actorOf(Props(Node(1, m, Map.empty, None, internal)), "n1")
      val n3: ActorRef =
        system.actorOf(Props(Node(3, m, Map.empty, None, internal)), "n3")

      val info0 = NodeInfo(n0, 0)
      val info1 = NodeInfo(n1, 1)
      val info3 = NodeInfo(n3, 3)

      val ft0 =
        Map(
          BigInt(1) -> info1,
          BigInt(2) -> info3,
          BigInt(4) -> info0
        )
      val ft1 =
        Map(
          BigInt(2) -> info3,
          BigInt(3) -> info3,
          BigInt(5) -> info0
        )
      val ft3 =
        Map(
          BigInt(4) -> info0,
          BigInt(5) -> info0,
          BigInt(7) -> info0
        )

      p.send(n0, UpdateFingerTableCertain(ft0))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n0))

      p.send(n1, UpdateFingerTableCertain(ft1))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n1))

      p.send(n3, UpdateFingerTableCertain(ft3))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n3))

      //ref-ы будут p.ref

      val n6: ActorRef =
        system.actorOf(
          Props(Node(6, m, Map.empty, None, "initializing")),
          "n6"
        )
      val info6 = NodeInfo(n6, 6)

      val pinfo0 = NodeInfo(p.ref, 0)
      val pinfo3 = NodeInfo(p.ref, 3)

      val ft6 =
        Map(
          BigInt(7) -> pinfo0,
          BigInt(0) -> pinfo0,
          BigInt(2) -> pinfo3
        )

      p.send(n6, Resolved(p.ref))
      p.expectMsg(FindPredecessor(7, n6, None, None))

      p.send(n0, FindPredecessor(7, p.ref, None, None))
      p.expectMsg(FoundPredecessor(7, info3, info0, None, None))

      p.send(n6, FoundPredecessor(7, pinfo3, pinfo0, None, None))
      p.expectMsg(5.seconds, GetPredecessor("stabilize"))

      p.send(n6, GotPredecessor(Some(pinfo3), "stabilize"))
      p.expectMsg(5.seconds, Notify(info6))
    }

    "find successor and predecessor" in {
      val p = TestProbe()

      val m = 3

      val internal = "internalReceive"

      val n0: ActorRef =
        system.actorOf(Props(Node(0, m, Map.empty, None, internal)))
      val n1: ActorRef =
        system.actorOf(Props(Node(1, m, Map.empty, None, internal)))
      val n3: ActorRef =
        system.actorOf(Props(Node(3, m, Map.empty, None, internal)))

      val info0 = NodeInfo(n0, 0)
      val info1 = NodeInfo(n1, 1)
      val info3 = NodeInfo(n3, 3)

      val ft0 =
        Map(
          BigInt(1) -> info1,
          BigInt(2) -> info3,
          BigInt(4) -> info0
        )
      val ft1 =
        Map(
          BigInt(2) -> info3,
          BigInt(3) -> info3,
          BigInt(5) -> info0
        )
      val ft3 =
        Map(
          BigInt(4) -> info0,
          BigInt(5) -> info0,
          BigInt(7) -> info0
        )

      p.send(n0, UpdateFingerTableCertain(ft0))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n0))

      p.send(n1, UpdateFingerTableCertain(ft1))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n1))

      p.send(n3, UpdateFingerTableCertain(ft3))
      p.expectMsg(FingerTableCertainUpdateSuccessful(n3))

      def predecessorTest(
          node: ActorRef,
          id: BigInt,
          i1: NodeInfo,
          i2: NodeInfo
      ) = {
        p.send(node, FindPredecessor(id, p.ref, None, None))
        p.expectMsg(FoundPredecessor(id, i1, i2, None, None))
      }

      def successorTest(
          node: ActorRef,
          id: BigInt,
          i1: NodeInfo,
          i2: NodeInfo
      ) = {
        p.send(node, FindSuccessor(id, p.ref, None))
        p.expectMsg(Successor(i1, i2, None))
      }

      predecessorTest(n0, BigInt(6), info3, info0)
      successorTest(n0, BigInt(6), info0, info3)

      predecessorTest(n1, BigInt(6), info3, info0)
      successorTest(n1, BigInt(6), info0, info3)

      predecessorTest(n3, BigInt(6), info3, info0)
      successorTest(n3, BigInt(6), info0, info3)

      //=========

      predecessorTest(n0, BigInt(1), info0, info1)
      successorTest(n0, BigInt(1), info1, info0)

      predecessorTest(n1, BigInt(1), info0, info1)
      successorTest(n1, BigInt(1), info1, info0)

      predecessorTest(n3, BigInt(1), info0, info1)
      successorTest(n3, BigInt(1), info1, info0)

      //===========

      predecessorTest(n0, BigInt(2), info1, info3)
      successorTest(n0, BigInt(2), info3, info1)

      predecessorTest(n1, BigInt(2), info1, info3)
      successorTest(n1, BigInt(2), info3, info1)

      predecessorTest(n3, BigInt(2), info1, info3)
      successorTest(n3, BigInt(2), info3, info1)

      system.terminate()
    }

  }
}
