package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout

import java.util.concurrent.atomic.AtomicBoolean
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

  implicit val timeout = Timeout(100.millis)

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {

    case message: Replicate =>
      log.debug(s"Replicator: Received message $message from ${sender.path}")
      replicate(sender, message)

    //    case SnapshotAck(_, seq) =>
    //      val entry = acks.get(seq)
    //      acks = acks.removed(seq)
    //      entry.foreach {
    //        case (replicaRef, Replicate(key, _, id)) => replicaRef ! Replicated(key, id)
    //      }
  }


  protected def replicate(leader: ActorRef, replicate: Replicate): Unit = {
    val snapshot = Snapshot(replicate.key, replicate.valueOption, nextSeq())
    val shouldRetry = new AtomicBoolean(true)
    context.system.scheduler.scheduleOnce(1.second) {
      shouldRetry.set(false)
    }

    def sendSnapshot(): Unit = {
      log.debug(s"Replicator: Sending $snapshot to replica ${replica.path}")
      (replica ? snapshot) (Timeout(100.millis)).onComplete {
        case Failure(exception) if exception.isInstanceOf[AskTimeoutException] =>
          log.debug(s"Replicator: received Timeout by $snapshot. Resending? ${shouldRetry}")
          if (shouldRetry.get()) sendSnapshot()
        case Success(m@SnapshotAck(key, id)) =>
          log.debug(s"Replicator: Get $m from replica ${replica.path}. Sending Replicated to leader ${leader.path}")
          leader ! Replicated(key, id)
      }
    }

    sendSnapshot()

  }
}
