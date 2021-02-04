package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{AskTimeoutException, ask, pipe}
import akka.util.Timeout
import kvstore.Arbiter._
import kvstore.Persistence.Persisted
import kvstore.Replicator.{Replicate, Snapshot, SnapshotAck}

import scala.concurrent.duration.DurationInt
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

  sealed trait MySnapshotPersistenceOperation

  case class MyRetrySnapshotPersistence(replicator: ActorRef, key: String, optValue: Option[String], seq: Long) extends MySnapshotPersistenceOperation

  case class MyPersistenceConfirmed(replicator: ActorRef, key: String, seq: Long) extends MySnapshotPersistenceOperation

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
  var persister: Option[ActorRef] = None
  var replicationCounter = 0L
  var replications = Map.empty[ActorRef, Set[Long]]
  var snapshotExpectedSeqs: Map[ActorRef, Long] = Map.empty[ActorRef, Long].withDefaultValue(0)
  var myPersistedExpected: Map[(String, Long), ActorRef] = Map.empty

  def nextReplicationCounter() = {
    val value = replicationCounter
    replicationCounter += 1
    value
  }

  implicit val timeout = Timeout(1.second)

  import context.dispatcher

  arbiter ! Join

  def receive = {
    case JoinedPrimary =>
      persister = Some(context.actorOf(persistenceProps))
      context.become(leader)
    case JoinedSecondary =>
      persister = Some(context.actorOf(persistenceProps))
      context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {

    case Insert(key, value, id) =>
      kv = kv.updated(key, value)
      persist(key, Some(value), id, sender)
    case Remove(key, id) =>
      kv = kv.removed(key)
      persist(key, None, id, sender)
    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)
    case Replicas(replicas) =>
      replicas
        .find { replica => !secondaries.contains(replica) }
        .foreach { replica =>
          val replicator = context.actorOf(Replicator.props(replica))
          //          replicators += replicator
          //          secondaries += (replica, replicator)
          val counter = nextReplicationCounter()
          //          replications += (replicator, Set.empty)
          kv.foreach {
            case (key, value) =>
              replicator ! Replicate(key, Some(value), counter)
            //replications.
          }
        }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {

    case Get(key, id) =>
      sender ! GetResult(key, kv.get(key), id)

    case Snapshot(key, optValue, seq) => // sender: Replicator
      val replicator = sender
      val expectedSeq = snapshotExpectedSeqs(replicator)
      if (seq == expectedSeq) {
        myPersistedExpected = myPersistedExpected.updated((key, seq), replicator)
        snapshotExpectedSeqs = snapshotExpectedSeqs.updated(replicator, seq + 1)
        retryPersistOnSnapshot(key, optValue, seq, replicator)
        optValue match {
          case Some(value) => kv = kv.updated(key, value)
          case None => kv = kv.removed(key)
        }
      } else if ((seq < expectedSeq)) {
        //snapshotExpectedSeqs = snapshotExpectedSeqs.updated(replicator, seq + 1)
        replicator ! SnapshotAck(key, seq)
      }

    case MyRetrySnapshotPersistence(replicator, key, optValue, seq) =>
      log.debug(s"MyRetrySnapshotPersistence: key=$key, value=$optValue, seq=$seq")
      retryPersistOnSnapshot(key, optValue, seq, replicator)

    case MyPersistenceConfirmed(replicator, key, seq) =>
      val replicatorOpt = myPersistedExpected.get((key, seq))
      if (replicatorOpt.isDefined) {
        replicatorOpt.get ! SnapshotAck(key, seq)
        myPersistedExpected = myPersistedExpected.removed((key, seq))
      }

    case Persisted(key, seq) =>
      val replicatorOpt = myPersistedExpected.get((key, seq))
      if (replicatorOpt.isDefined) {
        replicatorOpt.get ! SnapshotAck(key, seq)
        myPersistedExpected = myPersistedExpected.removed((key, seq))
      }
  }

  protected def retryPersistOnSnapshot(key: String, optValue: Option[String], seq: Long, replicator: ActorRef): Unit = {
    implicit val timeout: Timeout = Timeout(100.millis)

    (persister.get ? Persistence.Persist(key, optValue, seq))
      .map { _ => MyPersistenceConfirmed(replicator, key, seq) }
      .recover {
        case _: AskTimeoutException => MyRetrySnapshotPersistence(replicator, key, optValue, seq)
      } pipeTo self
  }

  def persist(key: String, value: Option[String], id: Long, applicant: ActorRef): Unit = {
    (persister.get ? Persistence.Persist(key, value, id)).onComplete {
      case Success(_) => applicant ! OperationAck(id)
      case Failure(_) => applicant ! OperationFailed(id)
    }
  }
}

