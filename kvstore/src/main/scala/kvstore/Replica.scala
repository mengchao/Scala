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
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // the current expected seq sent from Replicator (for Secondary Replica)
  var currentExpectedSeq = 0
  // map from sequence number to pair of sender and response
  var pendingPersistenceAcks = Map.empty[Long, (ActorRef, Object, Object)]
  
  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 5) {
    case _: Exception =>  {SupervisorStrategy.Restart}
  }

  var persistence = context.actorOf(persistenceProps)
  
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv = kv.updated(key, value)
      pendingPersistenceAcks = pendingPersistenceAcks.updated(id,
          (sender, OperationAck(id), OperationFailed(id)))
      self ! PersistenceTimeOut(id, 0, Persist(key, Some(value), id))
    }
    case Remove(key, id) => {
      kv -= key
      pendingPersistenceAcks = pendingPersistenceAcks.updated(id,
          (sender, OperationAck(id), OperationFailed(id)))
      self ! PersistenceTimeOut(id, 0, Persist(key, None, id))
    }
    case Get(key, id) => onGet(key, id)
    case Terminated(_) => onTerminated
    case Persisted(key, id) => onPersistenceCompleted(key, id)
    case PersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
      => onPersistenceTimeOut(id, nbrOfTimes, Persist(key, valueOption, opId))
  }

  /* TODO Behavior for the replica role. */
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
      val (sender, ack, failResponse) = pendingPersistenceAcks(id)
      pendingPersistenceAcks -= id
      sender ! ack
    }
  }
  
  def onPersistenceTimeOut(id: Long, nbrOfTimes: Int,
                           persistenceRequest: Persist): Unit = {
    if (pendingPersistenceAcks.contains(id)) {
      val (sender, ack, failResponse) = pendingPersistenceAcks(id)
      if (nbrOfTimes >= 9) {
        pendingPersistenceAcks -= id
        if (failResponse != null) {
          sender ! failResponse
        }
      }
      else {
        persistence ! persistenceRequest
        context.system.scheduler.scheduleOnce(100 milliseconds, self, 
            PersistenceTimeOut(id, nbrOfTimes + 1, persistenceRequest))
      }
    }
  }
}
