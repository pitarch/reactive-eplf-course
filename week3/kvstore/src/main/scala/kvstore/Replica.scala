package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.concurrent.duration.{Duration, DurationInt}

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))

}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {

  import Replica._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // mine
  var myPersister: Option[ActorRef] = None
  implicit val timeout: Timeout = Timeout(1.second)

  import context.dispatcher

  arbiter ! Join

  def receive = {

    case JoinedPrimary =>
      myPersister = Some(context.actorOf(persistenceProps))
      context.become(leader)

    case JoinedSecondary =>
      myPersister = Some(context.actorOf(persistenceProps))
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  // //////////////////////////////////////////////////////////////////////////
  // SECONDARY REPLICA
  // //////////////////////////////////////////////////////////////////////////

  case class PendingUpdate(key: String, id: Long, persisted: Boolean, replicators: Set[ActorRef],
                           globalCancellation: Cancellable, ackRef: ActorRef,
                           persistCancellation: Cancellable = Cancellable.alreadyCancelled, replicationId: Long)

  case class CancelUpdate(key: String, id: Long)

  case class RetryPersist(persist: Persist)

  var pendingUpdates: List[PendingUpdate] = List.empty

  val leader: Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case insert@Insert(key, value, id) =>
      log.debug(s"Replica/Leader: received $insert")
      kv = kv.updated(key, value)
      processUpdateOnLeader(key, id, Some(value))

    case remove@Remove(key, id) =>
      log.debug(s"Replica/Leader: received $remove")
      kv = kv.removed(key)
      processUpdateOnLeader(key, id, None)

    case persisted@Persisted(key, id) =>
      log.debug(s"Replicate/Leader: Received message: $persisted")

      val (targets, others) = pendingUpdates.partition { pending => pending.key == key && pending.id == id }
      val newTargets = targets.tapEach(_.persistCancellation.cancel()).map(_.copy(persisted = true))

      val (finished, stillPending) = newTargets.partition { pending => pending.persisted && pending.replicators.isEmpty }
      finished.foreach { pending =>
        pending.globalCancellation.cancel()
        pending.ackRef ! OperationAck(id)
      }
      pendingUpdates = others ++ stillPending

    case replicated@Replicated(key, id) =>
      log.debug(s"Replicate/Leader: Received $replicated. sent by ${sender.path}")

      val (targets, others) = pendingUpdates.partition { pending => pending.key == key && pending.id == id }
      val newTargets = targets.map { pending => pending.copy(replicators = pending.replicators.removedAll(Seq(sender))) }

      val (finished, stillPending) = newTargets.partition { pending => pending.persisted && pending.replicators.isEmpty }
      finished.foreach { pending =>
        pending.ackRef ! OperationAck(id)
        pending.globalCancellation.cancel()
      }
      pendingUpdates = others ++ stillPending

    case cancel@CancelUpdate(key, id) =>
      log.debug(s"Replicate/Leader: Received $cancel")
      pendingUpdates = pendingUpdates.filterNot { pending => pending.key == key && pending.id == id }

    case m@Replicas(replicas) =>
      log.debug(s"Replica/Leader: received $m")

      val removedReplicas = secondaries.keys.filterNot(replicas.contains).toSet
      val (activeReplicas, newReplicas) = replicas.filterNot(_ == self).partition(secondaries.contains)

      // stop removed replicators and remove them from secondaries
      secondaries
        .filter { case (replica, _) => removedReplicas.contains(replica) }
        .values
        .foreach { replicator =>
          log.debug(s"Stopping replicator ${replicator.path}")
          replicator ! PoisonPill
        }
      secondaries = secondaries.removedAll(removedReplicas)

      // create new replicators
      secondaries = secondaries ++ newReplicas.map { replica => replica -> context.actorOf(Replicator.props(replica)) }

      kv.foreach {
        case (key, value) => newReplicas.foreach { secondary =>
          val replicator = secondaries(secondary)
          val replicationId = nextReplicationId()
          log.debug(s"...Replica/Leader: sending Replicate to replicator ${replicator.path}")
          replicator ! Replicate(key, Some(value), replicationId)
        }
      }
  }

  private def processUpdateOnLeader(key: String, id: Long, optValue: Option[String]) = {
    val clientRef = sender
    val myself = self
    val persist = Persist(key, optValue, id)
    val replicate = Replicate(key, optValue, id)
    val replicators = secondaries.values.toSet
    val persistCancellation = context.system.scheduler.scheduleWithFixedDelay(Duration.Zero, 100.millis, myPersister.get, persist)
    replicators.foreach { replicator =>replicator ! replicate}

    val globalCancellation = context.system.scheduler.scheduleOnce(1.second) {
      log.debug(s"Replica/Leader: Global Cancellation triggered for replication $replicate")
      persistCancellation.cancel()
      myself ! CancelUpdate(key, id)
      val operationFailed = OperationFailed(id)
      log.debug(s"Replica/Leader: ... sending $operationFailed for Replicated seq=${replicationId} to ${clientRef.path}")
      clientRef ! operationFailed
    }

    val pendingUpdate = PendingUpdate(key, id, persisted = false, replicators, globalCancellation, sender, persistCancellation, replicationId)
    pendingUpdates = pendingUpdate +: pendingUpdates
  }


  var replicationId: Long = 0L

  def nextReplicationId(): Long = {
    val id = replicationId
    replicationId = replicationId + 1
    id
  }


  // //////////////////////////////////////////////////////////////////////////
  // SECONDARY REPLICA
  // //////////////////////////////////////////////////////////////////////////

  case class PendingSnapshot(key: String, seq: Long, ackRef: ActorRef, retry: Cancellable = Cancellable.alreadyCancelled)

  var expectedSnapshotSeq: Long = 0L

  var pendingSnapshots: List[PendingSnapshot] = List.empty

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case snapshot@Snapshot(key, optValue, seq) => // sender: Replicator
      log.debug(s"Replica/Secondary: received $snapshot. expected Seq=$expectedSnapshotSeq")

      val expectedSeq = expectedSnapshotSeq
      if (seq < expectedSeq) {
        log.debug(s"Replica/Secondary: Already ack $snapshot")
        sender ! SnapshotAck(key, seq)
      } else if (seq > expectedSeq) {
        log.debug(s"Replica/Secondary: Unexpected $snapshot due to a greater sequence")
        // nothing
      } else {
        val alreadyExists = pendingSnapshots.filter { pending => pending.key == key && pending.seq == seq }
        if (alreadyExists.isEmpty) {
          log.debug(s"Replica/Secondary: Trying to persist $snapshot")
          kv = if (optValue.isDefined) kv.updated(key, optValue.get) else kv.removed(key)
          val persist = Persist(key, optValue, seq)
          val retryCancellable = context.system.scheduler.scheduleWithFixedDelay(Duration.Zero, 100.millis, myPersister.get, persist)
          val pending = PendingSnapshot(key, seq, sender, retryCancellable)
          pendingSnapshots = pending +: pendingSnapshots
        } else {
          log.debug(s"Replica/Secondary: Ignored| Duplicated $snapshot")
        }
      }

    case persisted@Persisted(key, id) =>
      log.debug(s"Replica/Secondary: received $persisted")
      val maybeSnapshot = pendingSnapshots.find { pending => pending.key == key && pending.seq == id }
      if (maybeSnapshot.isDefined) {
        val pending = maybeSnapshot.get
        pending.retry.cancel()
        val ack = SnapshotAck(key, id)
        log.debug(s"Replicate/Secondary: ... sending $ack to ${pending.ackRef.path} ")
        pending.ackRef ! ack
        pendingSnapshots = pendingSnapshots.toSet.removedAll(Seq(pending)).toList
        expectedSnapshotSeq = List(expectedSnapshotSeq, id+1).max
      } else {
        log.debug(s"Replicate/Secondary: ... ignoring reaction to $persisted")
      }
  }
}

