package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

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

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  case class PersistenceTimeOut(seq: Long, nbrOfTimes: Int, persistenceRequest: Persist)
  case class ReplyIfOperationFailed(client: ActorRef, id: Long)
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the current expected seq sent from Replicator (for Secondary Replica)
  var currentExpectedSeq = 0
  // map from sequence number to pair of sender and response
  var pendingPersistenceAcks = Map.empty[Long, (ActorRef, Object, Object)]
  // map from sequence number to pair of sender and child replicators
  var pendingReplicationAcks = Map.empty[Long, (ActorRef, Set[ActorRef])]
  
  var isPrimary = false
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
    case _: Exception =>  {SupervisorStrategy.Restart}
  }

  var persistence = context.actorOf(persistenceProps)
  
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => {
      isPrimary = true
      context.become(leader)
    }
    case JoinedSecondary => {
      isPrimary = false
      context.become(replica)
    }
  }

  val leader: Receive = {
    case Replicas(replicas) => {
      val oldReplicators = secondaries.values.toSet
      secondaries = Map.empty[ActorRef, ActorRef]
      replicas filter(_  != self) foreach ( replica =>
        secondaries += ((replica, context.actorOf(Replicator.props(replica))))
      )
      replicators = secondaries.values.toSet
      val removedReplicators = oldReplicators.filter(!replicators.contains(_))
      val addedReplicators = replicators.filter(!oldReplicators.contains(_))
      replicateToNewAddedReplicas(addedReplicators)
      removeReplicasFromPendingList(removedReplicators)
      stopRemovedReplicators(removedReplicators)
    }
    case Insert(key, value, id) => {
      kv = kv.updated(key, value)
      pendingPersistenceAcks = pendingPersistenceAcks.updated(id,
          (sender, OperationAck(id), ReplyIfOperationFailed(sender, id)))
      replicators = secondaries.values.toSet
      if (!replicators.isEmpty)
      {
        pendingReplicationAcks += ((id, (sender, replicators)))
      }
      self ! PersistenceTimeOut(id, 0, Persist(key, Some(value), id))
      secondaries.values.foreach(_ ! Replicate(key, Some(value), id))
      context.system.scheduler.scheduleOnce(1 second, self,
          ReplyIfOperationFailed(sender, id))
    }
    case Remove(key, id) => {
      kv -= key
      pendingPersistenceAcks = pendingPersistenceAcks.updated(id,
          (sender, OperationAck(id), ReplyIfOperationFailed(sender, id)))
      replicators = secondaries.values.toSet
      if (!replicators.isEmpty)
      {
        pendingReplicationAcks += ((id, (sender, replicators)))
      }
      self ! PersistenceTimeOut(id, 0, Persist(key, None, id))
      secondaries.values.foreach(_ ! Replicate(key, None, id))
      context.system.scheduler.scheduleOnce(1 second, self,
          ReplyIfOperationFailed(sender, id))
    }
    case Get(key, id) => onGet(key, id)
    case Terminated(_) => onTerminated
    case Persisted(key, id) => onPersistenceCompleted(key, id)
    case PersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
      => onPersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
    case Replicated(key, id) => {
      onReplicated(id, sender)
    }
    case ReplyIfOperationFailed(client, id) => {
      if (!isUpdateOnPrimaryReplicaSucceed(id)) {
        pendingPersistenceAcks -= id
        pendingReplicationAcks -= id
        client ! OperationFailed(id)
      }
    }
  }

  val replica: Receive = {
    case Snapshot(key, valueOption, seq) => {
      if (seq > currentExpectedSeq) {
        /* ignore, no state change and no reaction */
      } else {
        if (seq == currentExpectedSeq) {
          valueOption match {
            case Some(value) => kv = kv.updated(key, value)
            case None => kv -= key
          }
          currentExpectedSeq += 1
        }
        pendingPersistenceAcks =
          pendingPersistenceAcks.updated(seq, (sender, SnapshotAck(key, seq), null))
        self ! PersistenceTimeOut(seq, 0, Persist(key, valueOption, seq))
      }
    }
    case Get(key, id) => onGet(key, id)
    case Terminated(_) => onTerminated
    case Persisted(key, id) => onPersistenceCompleted(key, id)
    case PersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
      => onPersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
  }
  
  def onGet(key: String, id: Long): Unit = {
    sender ! GetResult(key, kv.get(key), id)
  }
  
  def onTerminated: Unit = {
    context.stop(persistence)
    persistence = context.actorOf(persistenceProps)    
  }
  
  def onPersistenceCompleted(key: String, id: Long): Unit = {
    if (pendingPersistenceAcks.contains(id)) {
      val (client, ack, failResponse) = pendingPersistenceAcks(id)
      pendingPersistenceAcks -= id
      if (!isPrimary || isUpdateOnPrimaryReplicaSucceed(id)) {
        client ! ack
      }
    }
  }
  
  def isUpdateOnPrimaryReplicaSucceed(id: Long): Boolean = {
    !pendingPersistenceAcks.contains(id) &&
    !pendingReplicationAcks.contains(id)
  }
  
  def onPersistenceTimeOut(id: Long, nbrOfTimes: Int,
                           persistenceRequest: Persist): Unit = {
    if (pendingPersistenceAcks.contains(id)) {
      val (client, ack, failResponse) = pendingPersistenceAcks(id)
      if (nbrOfTimes >= 9) {
        if (failResponse != null) {
          client ! failResponse
        } else {
          pendingPersistenceAcks -= id
        }
      }
      else {
        persistence ! persistenceRequest
        context.system.scheduler.scheduleOnce(100 milliseconds, self, 
            PersistenceTimeOut(id, nbrOfTimes + 1, persistenceRequest))
      }
    }
  }
  
  def replicateToNewAddedReplicas(addedReplicators: Set[ActorRef]) = {
    addedReplicators.foreach(replicator =>
      kv.foreach(item => {
        val (k, v) = item
        replicator ! Replicate(k, Some(v), -1)
      }))
  }
  
  def removeReplicasFromPendingList(removedReplicators: Set[ActorRef]) = {
    removedReplicators.foreach { removedReplica =>
      {
        pendingReplicationAcks.foreach { item =>
          val (id, (client, replicas)) = item
          if (replicas.contains(removedReplica)) {
            onReplicated(id, removedReplica)
          }
        }
      }
    }
  }
  
  def stopRemovedReplicators(removedReplicators: Set[ActorRef]) = {
    removedReplicators.foreach(context.stop(_))
  }
  
  def onReplicated(id: Long, sender: ActorRef): Unit = {
    if (pendingReplicationAcks.contains(id)
      && pendingReplicationAcks(id)._2.contains(sender)) {
      val (client, pendingRepAcks) = pendingReplicationAcks(id)
      val newPendingRepAcks = pendingRepAcks - sender
      if (newPendingRepAcks.isEmpty) {
        pendingReplicationAcks -= id
        if (isUpdateOnPrimaryReplicaSucceed(id)) {
          client ! OperationAck(id)
        }
      } else {
        pendingReplicationAcks = pendingReplicationAcks.updated(id, (client, newPendingRepAcks))
      }
    }    
  }
}
