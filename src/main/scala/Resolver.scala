import Resolver._
import akka.actor._

class Resolver extends Actor with ActorLogging {
  override def receive: Receive = {
    case r @ Resolve(path) =>
      log.debug(s"Resolver received $r")
      context.actorSelection(path) ! Identify(
        ResolveInfo(sender)
      )
    case iden @ ActorIdentity(
          ResolveInfo(client: ActorRef),
          Some(ref)
        ) =>
      log.debug(s"Resolver received $iden")
      val res = Resolved(ref)
      log.debug(s"Resolver returns $res")
      client ! res
    case iden @ ActorIdentity(
          ResolveInfo(client: ActorRef),
          None
        ) =>
      log.debug(s"Resolver received $iden")
      val res = NotResolved
      log.debug(s"Resolver returns $res")
      client ! res
  }
}

object Resolver {
  case class ResolveInfo(
      `ref`: ActorRef
  ) extends JsonSerializable
  case class Resolve(
      `path`: ActorPath
  ) extends JsonSerializable

  case class Resolved(
      `ref`: ActorRef
  ) extends JsonSerializable

  case object NotResolved extends JsonSerializable
}
