/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import actorbintree.BinaryTreeSet._
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {

  import BinaryTreeSet._

  def createRoot(name: String = "root"): ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true), name = name)

  var root = createRoot()

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal


  var counter = 0

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case m: Operation => root forward m
    case GC =>
      log.info(s"Set/normal: GC Received!!!!!!!!!!!!!!!!")
      counter += 1
      val newRoot = createRoot(s"newRoot$counter")
      context.become(garbageCollecting(newRoot), true)
      root ! CopyTo(newRoot)
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => // ignore
    case m: Operation =>
//      log.info(s"Set/garbageCollecting: Enqueueing operation: $m")
      pendingQueue = pendingQueue :+ m
    case CopyFinished =>
      log.info("Set/garbageCollecting: CopyFinished received")
      root = newRoot
      pendingQueue foreach {
        newRoot ! _
      }
      pendingQueue = Queue.empty[Operation]
      context.become(normal, true)
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {

  import BinaryTreeNode._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {

    case m@Insert(requester, id, curr) =>
      val leaf = if (curr > elem) Right else Left
      if (curr == elem) {

        removed = false
        log.info(s"Node/normal: Inserting $curr on current node (id=$id) -> resurrected")
        requester ! OperationFinished(id)

      } else if (!subtrees.contains(leaf)) {

        subtrees = subtrees.updated(leaf, context.actorOf(props(curr, initiallyRemoved = false), name = s"$elem!$leaf:$curr"))
        log.info(s"Node/normal: Inserted $curr on $leaf of element $elem (id=$id)")
        requester ! OperationFinished(id)

      } else subtrees(leaf) forward m

    case m@Contains(requester, id, curr) =>
      if (curr == elem) {
        log.info(s"Contains found: $curr for operation $id")
        requester ! ContainsResult(id, !removed)
      }
      else {
        val leaf = if (curr > elem) Right else Left
        if (!subtrees.contains(leaf)) {
          log.info(s"responding ContainsResult NO FOUND element $curr for (id=$id)")
          requester ! ContainsResult(id, false)
        }
        else subtrees(leaf) forward m
      }

    case m@Remove(requester, id, curr) =>
      if (curr == elem) {
        removed = true
        requester ! OperationFinished(id)
      } else {
        val leaf = if (curr > elem) Right else Left
        if (!subtrees.contains(leaf)) requester ! OperationFinished(id)
        else subtrees(leaf) forward m
      }

    case CopyTo(targetRoot) =>
      log.info(s"Node/normal: CopyTo removed=$removed")
      val shouldBecome = !removed || subtrees.nonEmpty
      if (!removed) targetRoot ! Insert(self, elem, elem)
      subtrees.values foreach {
        _ ! CopyTo(targetRoot)
      }
      if (!shouldBecome) sender ! CopyFinished
      if (shouldBecome) context.become(copying(subtrees.values.toSet, !removed))
  }


  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {

    case OperationFinished(id) =>
      log.info(s"Node/copying: OperationFinished with id=$id")
      if (expected.isEmpty) {
        context.parent ! CopyFinished
      } else context.become(copying(expected, true), true)

    case CopyFinished =>
      val newExpected = expected - sender()
      log.info(s"Node/copying: CopyFinished on element $elem -- expected=${newExpected.size} insertConfirmed=$insertConfirmed")
      if (newExpected.isEmpty && (insertConfirmed || removed)) context.parent ! CopyFinished
      else context.become(copying(newExpected, insertConfirmed), true)
  }
}
