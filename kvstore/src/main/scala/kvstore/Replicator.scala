package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  case class TimeOut(seq: Long)

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  // var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case Replicate(key, valueOption, id) => {
      val seq = nextSeq
      replica ! Snapshot(key, valueOption, seq)
      acks += (seq -> (sender, Replicate(key, valueOption, id)))
      context.system.scheduler.scheduleOnce(200 milliseconds, self, TimeOut(seq))
    }
    case SnapshotAck(key, seq) => {
      if (acks.contains(seq)) {
        val (client, Replicate(key, valueOption, id)) = acks(seq)
        client ! Replicated(key, id)
        acks -= seq
      }
    }
    case TimeOut(seq) => {
      if (acks.contains(seq)) {
        val (client, Replicate(key, valueOption, id)) = acks(seq)
        replica ! Snapshot(key, valueOption, seq)
        context.system.scheduler.scheduleOnce(200 milliseconds, self, TimeOut(seq))
      }
    }
  }

}
