package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.{Failure, Success}

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

  case class PendingUpdate(key: String, id: Long, persisted: Boolean, replicators: Set[ActorRef], cancellable: Cancellable, ackRef: ActorRef)

  case class CancelUpdate(key: String, id: Long)

  case class RetryPersist(persist: Persist)

  private var pendingUpdates: List[PendingUpdate] = List.empty

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

    case retry@RetryPersist(persist) =>
      log.debug(s"Replica/Leader: received $retry")
      if (pendingUpdates.exists { pending => pending.key == persist.key && pending.id == persist.id }) retryPersist(persist)

    case persisted@Persisted(key, id) =>
      log.debug(s"Replicate/Primary: Received message: $persisted")
      val optPending = pendingUpdates
        .find { pending => pending.key == key && pending.id == id }
        .map { pending => pending.copy(persisted = true) }

      ackUpdate(optPending)

    case replicated@Replicated(key, id) =>
      log.debug(s"Replicate/Leader: Received $replicated")
      val optPending = pendingUpdates
        .find { pending => pending.key == key && pending.id == id }
        .map { pending => pending.copy(replicators = pending.replicators.removedAll(Seq(sender))) }

      ackUpdate(optPending)

    case cancel@CancelUpdate(key, id) =>
      log.debug(s"Replicate/Leader: Received $cancel")
      pendingUpdates = pendingUpdates.filterNot { pending => pending.key == key && pending.id == id }

    case m@Replicas(replicas) =>
      log.debug(s"Replica/Leader: received $m")

      // kill removed secondary replicas
      secondaries.keys.filterNot(replicas.contains).foreach {
        _ ! PoisonPill
      }
      // find the new added secondary replicas and create a replicator for them
      val newSecondaries = replicas.filterNot(_ == self).filterNot(secondaries.contains)
      secondaries = secondaries ++ newSecondaries.map(secondary => secondary -> context.actorOf(Replicator.props(secondary)))

      val task = new ReplicationTask(nextReplicationId(), kv.keys.toSet, sender)
      replicationTask = Some(task)
      newSecondaries.foreach(task.addReplicator)
      // For each pair key-value, send a Replicate message to every replicator
      kv.foreach {
        case (key, value) => newSecondaries.foreach { secondary =>
          secondaries(secondary) ! Replicate(key, Some(value), task.replicationId)
        }
      }
  }

  private def processUpdateOnLeader(key: String, id: Long, optValue: Option[String]) = {
    val clientRef = sender
    val cancellable = context.system.scheduler.scheduleOnce(1.second) {
      self ! CancelUpdate(key, id)
      clientRef ! OperationFailed(id)
    }

    val pendingUpdate = PendingUpdate(key, id, persisted = false, secondaries.values.toSet, cancellable, sender)
    pendingUpdates = pendingUpdate +: pendingUpdates
    val persist = Persist(key, optValue, id)
    retryPersist(persist)
    pendingUpdate.replicators.foreach { replicator => replicator ! Replicate(key, optValue, id) }
  }


  private def ackUpdate(optPending: Option[PendingUpdate]) = {
    optPending.foreach { pending =>
      val finished = pending.persisted && pending.replicators.isEmpty
      if (finished) {
        pendingUpdates = pendingUpdates.filterNot { target => target.key == pending.key && target.id == pending.id }
        pending.ackRef ! OperationAck(pending.id)
      }
    }
  }

  private def retryPersist(persist: Persist) = {
    ((myPersister.get ? persist) (Timeout(100.millis), self))
      .recover { case _: AskTimeoutException => RetryPersist(persist) }
      .pipeTo(self)
  }


  var replicationId: Long = 0L
  var replicationTask: Option[ReplicationTask] = None

  private def nextReplicationId(): Long = {
    val id = replicationId
    replicationId = replicationId + 1
    id
  }

  class ReplicationTask(val replicationId: Long, val keys: Set[String], val ackRef: ActorRef) {
    private var pendingReplications: Map[ActorRef, Set[String]] = Map.empty

    def addReplicator(replicator: ActorRef): Unit = {
      pendingReplications = pendingReplications.updated(replicator, keys)
    }

    def removeReplication(replicator: ActorRef, key: String): Unit = {
      pendingReplications.updated(replicator, pendingReplications(replicator) - key)
    }

    def replicated: Boolean = pendingReplications.isEmpty
  }


  // //////////////////////////////////////////////////////////////////////////
  // SECONDARY REPLICA
  // //////////////////////////////////////////////////////////////////////////

  case class PendingSnapshot(key: String, seq: Long, ackRef: ActorRef, retry: Cancellable = Cancellable.alreadyCancelled)

  var expectedSnapshotSeq: Map[String, Long] = Map.empty.withDefaultValue(0L)
  var pendingSnapshots: List[PendingSnapshot] = List.empty

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case snapshot@Snapshot(key, optValue, seq) => // sender: Replicator
      log.debug(s"Replica/Secondary: received $snapshot")

      val expectedSeq = expectedSnapshotSeq(key)
      if (seq < expectedSeq) {
        log.debug(s"Replica/Secondary: Already ack $snapshot")
        sender ! SnapshotAck(key, seq)
      } else if (seq > expectedSeq) {
        log.debug(s"Replica/Secondary: Ignored $snapshot")
        // nothing
      } else {
        log.debug(s"Replica/Secondary: Trying to persist $snapshot")
        kv = if (optValue.isDefined)  kv.updated(key, optValue.get) else kv.removed(key)
        val persist = Persist(key, optValue, seq)
        val retryCancellable = context.system.scheduler.scheduleWithFixedDelay(Duration.Zero, 100.millis, myPersister.get, persist)
        val pending = PendingSnapshot(key, seq, sender, retryCancellable)
        pendingSnapshots = pending +: pendingSnapshots
      }

    case persisted@Persisted(key, id) =>
      log.debug(s"Replica/Secondary: received $persisted")
      val maybeSnapshot = pendingSnapshots.find { pending => pending.key == key && pending.seq == id }
      maybeSnapshot.foreach { pending =>
        pending.retry.cancel()
        expectedSnapshotSeq = expectedSnapshotSeq.updated(key, List(id+1, expectedSnapshotSeq(key)).max)
        pending.ackRef ! SnapshotAck(key, id)
        pendingSnapshots = pendingSnapshots.filterNot { target => target.key == pending.key && target.seq == pending.seq }
      }
  }

}

