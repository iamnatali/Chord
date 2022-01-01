import Resolver._
import akka.actor._

class Resolver extends Actor with ActorLogging {
  override def receive: Receive = {
    case r @ Resolve(path, clientIpPort) =>
      log.debug(s"Resolver received $r")
      context.actorSelection(path) ! Identify(
        ResolveInfo(sender, clientIpPort)
      )
    case iden @ ActorIdentity(
          ResolveInfo(client: ActorRef, clientIpPort: String),
          Some(ref)
        ) =>
      log.debug(s"Resolver received $iden")
      val res = Resolved(ref, clientIpPort)
      log.debug(s"Resolver returns $res")
      client ! res
    case iden @ ActorIdentity(
          ResolveInfo(client: ActorRef, clientIpPort: String),
          None
        ) =>
      log.debug(s"Resolver received $iden")
      val res = NotResolved(clientIpPort)
      log.debug(s"Resolver returns $res")
      client ! res
  }
}

object Resolver {
  case class ResolveInfo(
      `ref`: ActorRef,
      clientIpPort: String
  ) extends JsonSerializable
  case class Resolve(
      `path`: ActorPath,
      clientIpPort: String
  ) extends JsonSerializable

  case class Resolved(
      `ref`: ActorRef,
      clientIpPort: String
  ) extends JsonSerializable

  case class NotResolved(clientIpPort: String) extends JsonSerializable
}
