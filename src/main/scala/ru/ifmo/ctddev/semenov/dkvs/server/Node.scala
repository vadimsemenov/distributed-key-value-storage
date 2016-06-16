package ru.ifmo.ctddev.semenov.dkvs.server

import akka.actor.{Actor, LoggingFSM, Props}
import ru.ifmo.ctddev.semenov.dkvs.client.RaftClient.RaftResponse
import ru.ifmo.ctddev.semenov.dkvs.log.LogItem
import ru.ifmo.ctddev.semenov.dkvs.protocol._

import scala.concurrent.duration._

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Node(startingMeta: Metadata) extends Actor with LoggingFSM[Role, Metadata] {

  import Node._

  startWith(Follower, startingMeta)

  resetElectionTimer()

  final val VALUE = "VALUE %s %s"
  final val NOT_FOUND = "NOT_FOUND"
  final val STORED = "STORED"
  final val DELETED = "DELETED"
  final val PONG = "PONG"

  when(Follower) {
    case Event(request: APPEND_ENTRY, meta)                                                         => // TODO: check
      if (meta.term > request.term) {
        sender ! APPEND_ENTRY_RESPONSE(meta.term, meta.id, success = false)
      } else {
        resetElectionTimer()
        val success = appendEntry(request, meta)
        sender ! APPEND_ENTRY_RESPONSE(meta.term, meta.id, success)
      }
      stay
    case Event(request: REQUEST_VOTE, meta)                                                         => // TODO: check
      if (meta.term > request.term) {
        sender ! REQUEST_VOTE_RESPONSE(meta.term, voteGranted = false)
      } else {
        meta.updateTerm(request.term)
        val voteGranted = meta.votes.votedFor.getOrElse(request.candidateId) == request.candidateId &&
          meta.journal.lastTerm <= request.lastLogTerm &&
          meta.journal.lastIndex <= request.lastLogIndex
        if (voteGranted) meta.votes.voteFor(request.candidateId)
        sender ! REQUEST_VOTE_RESPONSE(meta.term, voteGranted)
      }
      stay
    case Event(command: Command, meta) if command.isInstanceOf[SET] || command.isInstanceOf[DELETE] =>
      meta.leader match {
        case Some(leader) =>
          leader forward command
        case None         =>
        // FIXME: lost data :/
        // maybe we can try to add record to local journal and goto(Candidate)
        // so we at least try to safe this record
      }
      stay
    case Event(ElectionTimeout, meta)                                                               =>
      goto(Candidate) using prepareForCandidate(meta)
  }

  when(Candidate) {
    // FIXME: ignores incoming client-requests
    case Event(REQUEST_VOTE_RESPONSE(term, voteGranted), meta) =>
      if (term < meta.term) {
        // outdated response, request for new votes
        sender ! REQUEST_VOTE(meta.term, meta.id, meta.journal.lastIndex, meta.journal.lastTerm)
        stay
      } else if (term > meta.term) {
        meta updateTerm term
        goto(Follower) using prepareForFollower(meta)
      } else {
        if (voteGranted) meta.votes.receiveNewVote()
        if (meta.votes.majority(meta.nodes.length)) goto(Leader) using prepareForLeader(meta)
        else stay
      }
    case Event(command: REQUEST_VOTE, meta)                    =>
      if (meta.updateTerm(command.term)) {
        sender ! REQUEST_VOTE_RESPONSE(meta.term, voteGranted = true)
        goto(Follower) using prepareForFollower(meta)
      } else {
        sender ! REQUEST_VOTE_RESPONSE(meta.term, voteGranted = false)
        stay
      }
    case Event(command: APPEND_ENTRY, meta)                    =>
      if (meta.term > command.term) {
        sender ! APPEND_ENTRY_RESPONSE(meta.term, meta.id, success = false)
      } else {
        val success = appendEntry(command, meta)
        sender ! APPEND_ENTRY_RESPONSE(meta.term, meta.id, success)
      }
      stay
    case Event(ElectionTimeout, meta)                          =>
      goto(Candidate) using prepareForCandidate(meta)
  }

  when(Leader) {
    case Event(command: SET, meta)                         =>
      meta put(command, () => sender ! RaftResponse(STORED))
      stay
    case Event(command: DELETE, meta)                      =>
      meta put(command, () => sender ! RaftResponse(DELETED))
      stay
    case Event(SendHeartbeat, meta)                        =>
      broadcastEntries(meta)
      stay
    case Event(APPEND_ENTRY_RESPONSE(term, followerId, success), meta) =>
      if (meta.updateTerm(term)) {
        goto(Follower) using prepareForFollower(meta)
      } else {
        assert(meta.leaderData.isDefined)
        if (success) {
          val (nextIndex, matchIndex) = meta.leaderData.get
          val idx = nextIndex(followerId)
          matchIndex(followerId) = idx
          nextIndex(followerId) = math.min(meta.journal.size, idx + 1)
          // try to update commitIndex
          val qty = nextIndex count (_ >= idx)
          if (2 * qty > meta.nodes.length) {
            meta.updateCommitIndex(idx)
          }
        } else {
          val nextIndex = meta.leaderData.get._1
          nextIndex(followerId) -= 1
        }
        stay
      }
    case Event(command: REQUEST_VOTE, meta)                =>
      if (meta updateTerm command.term) {
        goto(Follower) using prepareForFollower(meta) replying REQUEST_VOTE_RESPONSE(meta.term, voteGranted = true)
      } else {
        stay replying REQUEST_VOTE_RESPONSE(meta.term, voteGranted = false)
      }
  }

  whenUnhandled {
    case Event(PING, meta) =>
      stay replying RaftResponse(PONG)
    case Event(GET(key), meta) =>
      stay replying RaftResponse(get(key, meta))
  }

  onTransition {
    case Leader -> Follower => cancelTimer(heartbeatTimerName)
    case _ -> Follower => resetElectionTimer()
    case _ -> Candidate => resetElectionTimer()
  }

  private def resetElectionTimer() = setTimer(electionTimerName, ElectionTimeout, nextElectionTimeout())

  private def resetHeartbeatTimer() = setTimer(heartbeatTimerName, SendHeartbeat, nextHeartbeatTimeout)

  private def appendEntry(request: APPEND_ENTRY, meta: Metadata): Boolean = {
    assert(request.term <= meta.term)
    meta.updateTerm(request.term)
    meta.leader = Some(meta.nodes(request.leaderId)) // update current leader
    meta.journal(request.prevLogIndex) match {
      case None       =>
        false
      case Some(item) =>
        if (item.term == request.prevLogTerm) {
          meta.journal -= request.prevLogIndex + 1
          if (request.entry != null) meta.journal += request.entry
          if (request.leaderCommit > meta.commitIndex) {
            meta.updateCommitIndex(request.leaderCommit)
          }
          true
        } else {
          meta.journal -= request.prevLogIndex
          false
        }
    }
  }

  private def broadcastEntries(meta: Metadata): Unit = {
    assert(meta.leader contains meta.nodes(meta.id))
    assert(meta.leaderData.isDefined)
    meta.nodes.indices foreach (sendEntryTo(_, meta))
    resetHeartbeatTimer()
  }

  private def sendEntryTo(nodeId: Int, meta: Metadata): Unit = { // TODO: check
    val prevIndex = meta.leaderData.get._1(nodeId) - 1
    val prevTerm = meta.journal.termOf(prevIndex)
    val entry = meta.journal(prevIndex + 1) match {
      case Some(LogItem(term, command, _)) => LogItem(term, command, LogItem.void)
      case None                            => null
    }
    meta.nodes(nodeId) ! APPEND_ENTRY(meta.term, meta.id, prevIndex, prevTerm, entry, meta.commitIndex)
  }

  private def get(key: String, meta: Metadata): String = ((meta.map get key) map (VALUE.format(key, _))) getOrElse NOT_FOUND

  private def prepareForCandidate(meta: Metadata): Metadata = {
    meta.nextTerm()
    (meta.nodes withFilter (_ != meta.nodes(meta.id))) foreach {
      _ ! REQUEST_VOTE(meta.term, meta.id, meta.journal.lastIndex, meta.journal.lastTerm)
    }
    meta
  }

  private def prepareForLeader(meta: Metadata): Metadata = {
    log.info(s"Leader for term ${meta.term}")
    meta.leader = Some(meta.nodes(meta.id))
    meta.leaderData = Some((
      Array.fill[Int](meta.nodes.length)(meta.journal.size),
      Array.fill[Int](meta.nodes.length)(0)
    ))
    broadcastEntries(meta)
    meta
  }

  private def prepareForFollower(meta: Metadata): Metadata = {
    meta.votes = Votes.empty
    meta
  }

  initialize() // FSM requirement
}

object Node {
  val electionTimerName = "noHeartbeatTimer"
  val heartbeatTimerName = "heartbeatTimerName"

  // TODO: constants should be configurable
  def nextElectionTimeout(): FiniteDuration = (math.random * 100 + 300) millis
  def nextHeartbeatTimeout: FiniteDuration = 150 millis

  def props(startingMeta: Metadata) = Props(classOf[Node], startingMeta)
}