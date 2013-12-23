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
import scala.language.postfixOps

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
  case class OperationTimeOut(client: ActorRef, id: Long)
  case class HistoricalSnapshot(key: String, valueBefore: Option[String], id: Long)
  
  var kv = Map.empty[String, String]

  // map from sequence number to pair of sender and response
  var isPrimary = false
  var pendingPersistenceAcks = Map.empty[Long, (ActorRef, Object, Option[Object])]
  var historicalSnapshots = Vector.empty[HistoricalSnapshot]

  // For primary replica only
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // map from sequence number to pair of sender and child replicators
  var pendingReplicationAcks = Map.empty[Long, (ActorRef, Set[ActorRef])]
  
  // For secondary replica only
  // the current expected seq sent from Replicator (for Secondary Replica)
  var currentExpectedSeq = 0
  
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
    case Replicas(replicas) => onReplicas(replicas)
    case Insert(key, value, id) => onInsert(key, value, id)
    case Remove(key, id) => onRemove(key, id)
    case Get(key, id) => onGet(key, id)
    case Terminated(_) => onTerminated
    case Persisted(key, id) => onPersistenceCompleted(key, id)
    case PersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
      => onPersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
    case Replicated(key, id) => onReplicated(id, sender)
    case OperationTimeOut(client, id) => onOperationTimeOut(client, id)
    case _ =>
  }

  val replica: Receive = {
    case Snapshot(key, valueOption, seq) => onSnapshot(key, valueOption, seq)
    case Get(key, id) => onGet(key, id)
    case Terminated(_) => onTerminated
    case Persisted(key, id) => onPersistenceCompleted(key, id)
    case PersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
      => onPersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
  }
  
  // Primary Replica Operations
  def onReplicas(replicas: Set[ActorRef]): Unit = {
    val oldReplicators = secondaries.values.toSet
    
    secondaries = secondaries.filter(_ match {case (k, v) => replicas.contains(k)})
    replicas filter (_ != self) foreach (replica => {
      if (!secondaries.contains(replica)) {
        secondaries += (replica -> context.actorOf(Replicator.props(replica)))
      }
    })
    replicators = secondaries.values.toSet
    
    val removedReplicators = oldReplicators.filterNot(replicators.contains(_))
    val addedReplicators = replicators.filterNot(oldReplicators.contains(_))
    
    replicateToNewAddedReplicas(addedReplicators)
    removeReplicasFromPendingList(removedReplicators)
    stopRemovedReplicators(removedReplicators)    
  }
  
  def replicateToNewAddedReplicas(addedReplicators: Set[ActorRef]): Unit = {
    addedReplicators.foreach(replicator =>
      kv.foreach(item => {
        val (k, v) = item
        replicator ! Replicate(k, Some(v), -1)
      }))
  }
  
  def removeReplicasFromPendingList(removedReplicators: Set[ActorRef]): Unit = {
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
  
  def onReplicated(id: Long, sender: ActorRef): Unit = {
    if (pendingReplicationAcks.contains(id)
      /* original sender is client(not self) */
      && pendingReplicationAcks(id)._1 != self
      /* sender is in the pending replication acks list */
      && pendingReplicationAcks(id)._2.contains(sender)) {
      val (client, pendingRepAcks) = pendingReplicationAcks(id)
      val newPendingRepAcks = pendingRepAcks - sender
      if (newPendingRepAcks.isEmpty) {
        pendingReplicationAcks -= id
        if (isUpdateOnPrimaryReplicaSucceed(id)) {
          client ! OperationAck(id)
          clearSucceededHistoricalSnapshots(id)
        }
      } else {
        pendingReplicationAcks = pendingReplicationAcks.updated(id, (client, newPendingRepAcks))
      }
    }    
  }

  def isUpdateOnPrimaryReplicaSucceed(id: Long): Boolean = {
    !pendingPersistenceAcks.contains(id) &&
    !pendingReplicationAcks.contains(id)
  }
  
  def stopRemovedReplicators(removedReplicators: Set[ActorRef]): Unit = {
    removedReplicators.foreach(context.stop(_))
  }
  
  def onInsert(key: String, value: String, id: Long): Unit = {
    onChange(key, Some(value), id)
  }
  
  def onRemove(key: String, id: Long): Unit = {
    onChange(key, None, id)
  }
  
  def onChange(key: String, valueOption: Option[String], id: Long): Unit = {
    historicalSnapshots :+= HistoricalSnapshot(key, kv.get(key), id)
    valueOption match {
      case Some(value) => kv = kv.updated(key, value)
      case None => kv -= key
    }
    pendingPersistenceAcks += 
      (id -> (sender, OperationAck(id), Some(OperationTimeOut(sender, id))))
    replicators = secondaries.values.toSet
    if (!replicators.isEmpty) {
      pendingReplicationAcks += (id -> (sender, replicators))
    }
    self ! PersistenceTimeOut(id, 0, Persist(key, valueOption, id))
    secondaries.values.foreach(_ ! Replicate(key, valueOption, id))
    context.system.scheduler.scheduleOnce(1 second, self,
      OperationTimeOut(sender, id))
  }
  
  def onOperationTimeOut(client: ActorRef, id: Long): Unit = {
    if (!isUpdateOnPrimaryReplicaSucceed(id)) {
      pendingPersistenceAcks -= id
      pendingReplicationAcks -= id
      restoreValueForFailedOperation(id)
      client ! OperationFailed(id)
    }
  }
  
  // Secondary Replica Operations
  def onSnapshot(key: String, valueOption: Option[String], seq: Long): Unit = {
    if (seq > currentExpectedSeq) {
      /* ignore, no state change and no reaction */
    } else if (seq < currentExpectedSeq) {
      sender ! SnapshotAck(key, seq)
    } else {
      historicalSnapshots :+= HistoricalSnapshot(key, kv.get(key), seq)
      valueOption match {
        case Some(value) => kv = kv.updated(key, value)
        case None => kv -= key
      }
      currentExpectedSeq += 1
      pendingPersistenceAcks +=
        (seq -> (sender, SnapshotAck(key, seq), None))
      self ! PersistenceTimeOut(seq, 0, Persist(key, valueOption, seq))
    }
  }
  
  // Common Operations
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
        clearSucceededHistoricalSnapshots(id)
      }
    }
  }
  
  def onPersistenceTimeOut(id: Long, nbrOfTimes: Int,
                           persistenceRequest: Persist): Unit = {
    if (pendingPersistenceAcks.contains(id)) {
      val (client, ack, failResponse) = pendingPersistenceAcks(id)
      if (nbrOfTimes >= 9) {
        failResponse match {
          case Some(reponse) => {
            self ! failResponse
          }
          case None => {
            pendingPersistenceAcks -= id
            restoreValueForFailedOperation(id)
          }
        }
      }
      else {
        persistence ! persistenceRequest
        context.system.scheduler.scheduleOnce(100 milliseconds, self, 
            PersistenceTimeOut(id, nbrOfTimes + 1, persistenceRequest))
      }
    }
  }
  
  def clearSucceededHistoricalSnapshots(opId: Long): Unit = {
    discardOutDatedSnapshots(opId, false)
  }
  
  def restoreValueForFailedOperation(opId: Long): Unit = {
    discardOutDatedSnapshots(opId, true)
  }
  
  def discardOutDatedSnapshots(opId: Long, 
                               restoreSnapshotValueBefore: Boolean): Unit = {
    val currentSnapshots = historicalSnapshots.filter(_.id == opId)
    if (!currentSnapshots.isEmpty) {
      val currentSnapShot = currentSnapshots(0)
      currentSnapShot match {
        case HistoricalSnapshot(key, valueBefore, id) => {
          historicalSnapshots = historicalSnapshots.filterNot(s => s.key == key
            && s.id <= id)
          if (restoreSnapshotValueBefore) {
            valueBefore match {
              case Some(value) => kv = kv.updated(key, value)
              case None => kv -= key
            }
          }
        }
      }
    }
  }
}
