package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Persistence.{Persist, Persisted}
import kvstore.Replicator.{Replicate, Replicated, Snapshot, SnapshotAck}

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
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
  var myExpectedSnapshotSeq: Map[ActorRef, Long] = Map.empty[ActorRef, Long].withDefaultValue(0)
  var myRunningSnapshots: List[SnapshotTask] = List.empty
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
  val leader: Receive = {

    case m@Insert(key, value, id) =>
      log.debug(s"Replica/Leader: received $m")
      kv = kv.updated(key, value)
      processInsertOrRemoveOnLeader(sender, id, key, Some(value))

    case Remove(key, id) =>
      kv = kv.removed(key)
      processInsertOrRemoveOnLeader(sender, id, key, None)

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Replicas(replicas) =>
      val newReplicaReplicators = replicas
        .filterNot(_ == self)
        .filterNot(secondaries.contains)
        .map { replica => (replica, context.actorOf(Replicator.props(replica))) }
        .toMap


      newReplicaReplicators.foreach { case (replica, replicator) =>
        log.debug(s"Replica/Leader: Sending Replicate for all entries in the store to replicator [${replicator.path}]: ${kv}")
        kv.foreach {
          case (key, value) => replicator ! Replicate(key, Some(value), 0L)
        }
      }

      secondaries = secondaries ++ newReplicaReplicators

    case m@Persisted(key, id) =>
      log.debug(s"Replicate/Primary: Received message: $m")
  }

  def replicate(key: String, optValue: Option[String], id: Long) = {
    secondaries
      .filterNot { case (replica, _) => replica == self }
      .map { case (replica, replicator) =>
        val message = Replicate(key, optValue, id)
        log.debug(s"Replica/Leader: asking $message to replicator [${replicator.path}]")
        val future = replicator ? message
        future.foreach { case Replicated(key, id) => replica }
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case snapshot@Snapshot(key, optValue, seq) => // sender: Replicator
      val replicator = sender
      val expectedSeq = myExpectedSnapshotSeq(replicator)
      log.debug(s"Replicate/Secondary: received message $snapshot from replicator [${replicator.path}]. expected seq=${expectedSeq}")
      if (seq == expectedSeq) {
        log.debug(s"Updated local store (key=$key, value=$optValue)")
        optValue match {
          case Some(value) => kv = kv.updated(key, value)
          case None => kv = kv.removed(key)
        }
        myExpectedSnapshotSeq = myExpectedSnapshotSeq.updated(replicator, seq + 1)
        val task = new SnapshotTask(snapshot, replicator)
        myRunningSnapshots = task +: myRunningSnapshots
      } else if (seq < expectedSeq) {
        replicator ! SnapshotAck(key, seq)
      }

    case persisted: Persisted =>
      log.debug(s"Replica/Secondary: Received $persisted")
      myRunningSnapshots
        .filter(_.snapshot.seq == persisted.id)
        .foreach { task =>
          task.cancel()
          task.ackRef ! SnapshotAck(task.snapshot.key, task.snapshot.seq)
        }
      myRunningSnapshots = myRunningSnapshots.filterNot(_.snapshot.seq == persisted.id)
  }


  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def persistByInsert(persist: Persist, shouldRetry: AtomicBoolean, cancellable: Cancellable): Future[Boolean] = {
    val future = ask(myPersister.get, persist)(Timeout(100.millis))
    future.transformWith {
      case Failure(exception) if exception.isInstanceOf[AskTimeoutException] =>
        if (shouldRetry.get()) persistByInsert(persist, shouldRetry, cancellable) else Future.failed(null)
      case Success(value) =>
        cancellable.cancel()
        Future.successful(true)
    }
  }

  def processInsertOrRemoveOnLeader(clientRef: ActorRef, id: Long, key: String, optValue: Option[String]): Unit = {
    val shouldRetry = new AtomicBoolean(true)
    val persist = Persist(key, optValue, id)
    val replicate = Replicate(key, optValue, id)

    val cancellable = context.system.scheduler.scheduleOnce(1.second) {
      shouldRetry.set(false)
      log.debug(s"Replica/Leader: failed either $persist or $replicate")
      clientRef ! OperationFailed(id)
    }

    val persistFuture = persistByInsert(persist, shouldRetry, cancellable)
    val replicateFuture = processReplicateOnAllSecondaries(replicate)

    Future.sequence(Seq(persistFuture, replicateFuture)).onComplete {
      case Success(_) =>
        cancellable.cancel()
        log.debug(s"Replica/Leader: Successful both persist and replicate for $persist and $replicate")
        clientRef ! OperationAck(id)
      case Failure(_) =>
    }
  }

  def processReplicateOnAllSecondaries(replicate: Replicate) = {

    val futures = secondaries
      .filterNot(_._1 == null) // exclude the Leader Replica
      .keys
      .map { replicator: ActorRef =>
        log.debug(s"Replica/Leader: Replicating $replicate on replicate ${replicator.path}")
        (replicator ? replicate) map { _ => true }
      }

    Future.sequence(futures).map(_ => true)
  }


  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // SECONDARY REPLICA
  // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  class SnapshotTask(val snapshot: Snapshot, val ackRef: ActorRef) {

    private val persist = Persist(snapshot.key, snapshot.valueOption, snapshot.seq)
    private val cancellable = context.system.scheduler.scheduleWithFixedDelay(Duration.Zero, 100.millis) { new Runnable {
      override def run(): Unit = {
        log.debug(s"Sending Persist to actor ${ackRef.path}")
        myPersister.get ! persist
      }
    }}

    def cancel(): Boolean = {
      log.debug(s"Cancelling persist $persist for snapshot $snapshot")
      cancellable.cancel()
    }
  }

}

