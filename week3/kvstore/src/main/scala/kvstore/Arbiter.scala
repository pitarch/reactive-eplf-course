package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef}

object Arbiter {

  case object Join

  case object JoinedPrimary

  case object JoinedSecondary

  /**
    * This message contains all replicas currently known to the arbiter, including the primary.
    */
  case class Replicas(replicas: Set[ActorRef])

}

class Arbiter extends Actor with ActorLogging {

  import Arbiter._

  var leader: Option[ActorRef] = None
  var replicas = Set.empty[ActorRef]

  def receive = {
    case Join =>
      var targetMessage: Product = JoinedSecondary
      if (leader.isEmpty) {
        leader = Some(sender)
        targetMessage = JoinedPrimary
      }
      replicas += sender
      leader.get forward Replicas(replicas)
  }

}
