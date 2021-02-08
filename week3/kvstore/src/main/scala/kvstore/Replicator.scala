package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.util.Timeout

import scala.concurrent.duration.{Duration, DurationInt}

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

  case class RetryReplicate(replicate: Replicate, ackRef: ActorRef, retry: Cancellable = Cancellable.alreadyCancelled, seq: Long = 0)
  case class CancelReplicate(replicate: Replicate)

  private var pendingReplicates: List[RetryReplicate] = List.empty

  private var snapshotSeq: Long = 0L

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case replicate@Replicate(key, valueOption, id) =>
      log.debug(s"Replicator: received $replicate")
      val leaderRef = sender
      // remove older replications for this key having. They will be cancelled
      val alreadyExisting = pendingReplicates.find { pending => pending.replicate.key == key && pending.replicate.id  == id }

      if (alreadyExisting.isEmpty) {
        val snapshot = Snapshot(key, valueOption, snapshotSeq)
        snapshotSeq += 1
        val retry = context.system.scheduler.scheduleWithFixedDelay(Duration.Zero, 100.millis, replica, snapshot)
        val retryReplicate = RetryReplicate(replicate, leaderRef, retry, snapshot.seq)
        // send snapshot to replica and inject the reply to myself.
        log.debug(s"Replicator: ...Sending $snapshot to replica ${replica.path}")
        pendingReplicates = retryReplicate +: pendingReplicates
        replica ! snapshot
      } else {
        val pending = alreadyExisting.get
        val snapshot = Snapshot(key, valueOption, pending.seq)
        log.debug(s"Replicator: Already exist $replicate... resending $snapshot")
        replica ! snapshot
      }

    case snapshotAck@SnapshotAck(key, seq) =>
      log.debug(s"Replicator: received $snapshotAck")
      val ackedReplicates = pendingReplicates.filter { pending => pending.replicate.key == key && pending.seq == seq }
      pendingReplicates = pendingReplicates.toSet.removedAll(ackedReplicates).toList
      ackedReplicates.foreach { retryReplicate =>
        retryReplicate.retry.cancel()
        retryReplicate.ackRef ! Replicated(key, retryReplicate.replicate.id)
      }

    case CancelReplicate(replicate: Replicate) =>
      val targets = pendingReplicates.filter { pending => pending.replicate.key == replicate.key && pending.replicate.id == replicate.id }
      targets.foreach(_.retry.cancel())
      pendingReplicates = pendingReplicates.toSet.removedAll(targets).toList
  }

  override def postStop(): Unit = {
    log.debug(s"Stopping replicator")
  }
}
