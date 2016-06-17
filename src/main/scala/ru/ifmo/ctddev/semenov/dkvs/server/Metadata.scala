package ru.ifmo.ctddev.semenov.dkvs.server

import akka.actor.ActorSelection
import ru.ifmo.ctddev.semenov.dkvs.log.{Log, LogItem}
import ru.ifmo.ctddev.semenov.dkvs.protocol.{Command, DELETE, SET}

import scala.collection.mutable

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
// TODO: make meta immutable (is it even possible with hash-map inside?)
case class Metadata(id: Int,
                    // common persistent(?) data
                    var term: Int = 0,
                    var journal: Log,
                    map: mutable.Map[String, String] = mutable.Map.empty,
                    var nodes: Array[Metadata.NodeRef],
                    var leader: Option[Metadata.NodeRef] = None,
                    var votes: Votes = Votes.empty,
                    // common volatile(?) data
                    var commitIndex: Int = 0,
                    // var appliedIndex: Int = 0, // useless?
                    // exists only in leader-state
                    //                     nextIndex    matchIndex
                    //                     exact        inclusive
                    var leaderData: Option[(Array[Int], Array[Int])] = None) {
  def toCandidate() = votes = Votes.forCandidate(id)

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

  // commitIndex is exclusive
  def updateCommitIndex(newCommitIndex: Int) = {
    val next = math.min(newCommitIndex, journal.size)
//    assert(next == 0 || journal(next - 1).get.term == term,
//      s"commitIndex (next=$next) with illegal term=${journal(next - 1).get.term}")
    for (i <- commitIndex until next) journal(i) get match {
      case LogItem(_, SET(key, value), onStored) =>
        map += key -> value
        println(s"stored ($key,$value)")
        onStored()
      case LogItem(_, DELETE(key), onStored)     =>
        map remove key
        println(s"deleted ($key)")
        onStored()
    }
    commitIndex = math.max(commitIndex, next)
    // TODO: check!
  }

  def receiveNewVote() = votes = votes.receiveNewVote()

  def put(command: Command, onStored: () => Unit) = journal += LogItem(term, command, onStored)
}

object Metadata {
  def apply(id: Int, journal: Log, nodes: Array[NodeRef]): Metadata = Metadata(
    id = id,
    term = 0,
    journal = journal,
    map = mutable.Map.empty,
    nodes = nodes,
    leader = None,
    votes = Votes.empty,
    commitIndex = 0,
    leaderData = None
  )
  type NodeRef = ActorSelection
}

case class Votes(votedFor: Option[Int], withMe: Int) {
  def voteFor(candidate /*Trump*/ : Int) = Votes(votedFor orElse Some(candidate), withMe)

  def receiveNewVote() = Votes(votedFor, withMe + 1)

  def majority(size: Int): Boolean = size < 2 * withMe
}

object Votes {
  val empty = Votes(None, 0)

  def forCandidate(candidateId: Int) = Votes(Some(candidateId), 1)
}