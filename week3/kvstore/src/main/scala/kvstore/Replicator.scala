package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import java.util.concurrent.TimeoutException
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

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

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L

  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  implicit val timeout = Timeout(200.millis)

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case message@Replicate(key, valueOption, _) =>
      val seq = nextSeq()
      acks = acks.updated(seq, (sender, message))
      retrySnapshot(key, valueOption, seq) pipeTo self
    case SnapshotAck(_, seq) =>
      val entry = acks.get(seq)
      acks = acks.removed(seq)
      entry.foreach {
        case (replicaRef, Replicate(key, _, id)) => replicaRef ! Replicated(key, id)
      }
  }


  protected def retrySnapshot(key: String, optValue: Option[String], seq: Long): Future[Any] = {
    val future = replica ? Snapshot(key, optValue, seq)
    future.recoverWith {
      case t:TimeoutException => retrySnapshot(key, optValue, seq)
    }
  }


}
