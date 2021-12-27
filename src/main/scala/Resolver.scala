import Resolver._
import akka.actor._

class Resolver extends Actor with ActorLogging {
  override def receive: Receive = {
    case r @ Resolve(path, clientIpPort) =>
      log.debug(s"Resolver received $r")
      context.actorSelection(path) ! Identify(
        ResolveInfo(path, sender, clientIpPort)
      )
    case iden @ ActorIdentity(
          ResolveInfo(path: ActorPath, client: ActorRef, clientIpPort: String),
          Some(ref)
        ) =>
      log.debug(s"Resolver received $iden")
      val res = Resolved(path, ref, clientIpPort)
      log.debug(s"Resolver returns $res")
      client ! res
    case iden @ ActorIdentity(
          ResolveInfo(path: ActorPath, client: ActorRef, clientIpPort: String),
          None
        ) =>
      log.debug(s"Resolver received $iden")
      val res = NotResolved(path, clientIpPort)
      log.debug(s"Resolver returns $res")
      client ! res
  }
}

object Resolver {
  case class ResolveInfo(
      resolvedPath: ActorPath,
      ref: ActorRef,
      clientIpPort: String
  )

  case class Resolve(
      path: ActorPath,
      clientIpPort: String //можно вычленить из path
  ) extends JsonSerializable

  case class Resolved(
      resolvedPath: ActorPath,
      ref: ActorRef,
      clientIpPort: String
  ) extends JsonSerializable

  case class NotResolved(path: ActorPath, clientIpPort: String)
      extends JsonSerializable
}
