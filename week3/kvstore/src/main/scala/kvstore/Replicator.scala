package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.util.Timeout

import scala.concurrent.duration.DurationInt

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)

  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)

  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {

  import Replicator._
  import context.dispatcher

  implicit val timeout = Timeout(100.millis)

  case class RetryReplicate(replicate: Replicate, ackRef: ActorRef)

  private var pendingReplicates: List[RetryReplicate] = List.empty

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case replicate@Replicate(key, valueOption, id) =>
      log.debug(s"Replicator: received $replicate")
      val leaderRef = sender
      // remove older replications for this key having
      pendingReplicates = pendingReplicates.filterNot { pending => pending.replicate.key == key && pending.replicate.id <= id }

      val retryReplicate = RetryReplicate(replicate, leaderRef)
      // send snapshot to replica and inject the reply to myself.
      (replica ? Snapshot(key, valueOption, id))
        .recover { case _: AskTimeoutException => retryReplicate }
        .pipeTo(self)
      pendingReplicates = retryReplicate +: pendingReplicates

    case retryReplicate@RetryReplicate(m@Replicate(key, valueOption, id), replicator) =>
      log.debug(s"Replicator: received $retryReplicate")
      if (pendingReplicates.contains(retryReplicate))
        (replica ? Snapshot(key, valueOption, id))
          .recover { case _: AskTimeoutException => RetryReplicate(m, replicator)
          }.pipeTo(self)

    case snapshotAck@SnapshotAck(key, seq) =>
      log.debug(s"Replicator: received $snapshotAck")
      pendingReplicates
        .find { r => r.replicate.key == key && r.replicate.id == seq }
        .foreach { pending => pending.ackRef ! Replicated(key, seq)
        }
  }
}
