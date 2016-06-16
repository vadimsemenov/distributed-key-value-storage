package ru.ifmo.ctddev.semenov.dkvs.server

import scala.collection.mutable
import akka.actor.ActorSelection
import ru.ifmo.ctddev.semenov.dkvs.log.{Log, LogItem}
import ru.ifmo.ctddev.semenov.dkvs.protocol.{Command, DELETE, SET}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
// TODO: make meta immutable (is it even possible with hash-map inside?)
case class Metadata(
  id: Int,
              // common persistent(?) data
  var term: Int = 0,
  var journal: Log,
  map: mutable.Map[String, String] = mutable.Map.empty,
  var nodes: Array[Metadata.NodeRef],
  var leader: Option[Metadata.NodeRef] = None,
  var votes: Votes = Votes.empty,
              // common volatile(?) data
  var commitIndex: Int = 0,
//  var appliedIndex: Int = 0, // useless?
              // exists only in leader-state
  var leaderData: Option[(Array[Int], Array[Int])] = None
) {

  def nextTerm() = updateTerm(term + 1)

  def updateTerm(term: Int): Boolean = {
    if (this.term < term) {
      this.term = term
      votes = Votes.empty
      true
    } else {
      false
    }
  }

  def updateCommitIndex(leaderCommit: Int) = {
    val next = math.max(leaderCommit, journal.size)
    assert(journal(next).get.term == term, "update commitIndex with illegal term")
    for (i <- commitIndex + 1 to next) {
      journal(i) get match {
        case LogItem(_, SET(key, value), onStored) =>
          map += key -> value
          onStored()
        case LogItem(_, DELETE(key), onStored) =>
          map remove key
          onStored()
      }
    }
    // TODO: check!
  }

  def put(command: Command, onStored: () => Unit) = journal += LogItem(term, command, onStored)
}

object Metadata {
  type NodeRef = ActorSelection
}

case class Votes (votedFor: Option[Int], withMe: Int) {
  def voteFor(candidate/*Trump*/: Int) = Votes(votedFor orElse Some(candidate), withMe)
  def receiveNewVote() = Votes(votedFor, withMe + 1)
  def majority(size: Int): Boolean = size < 2 * withMe
}

object Votes {
  val empty = Votes(None, 0)
  def forCandidate(candidateId: Int) = Votes(Some(candidateId), 1)
}