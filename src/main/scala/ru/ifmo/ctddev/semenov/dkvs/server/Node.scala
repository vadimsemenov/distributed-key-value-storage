package ru.ifmo.ctddev.semenov.dkvs.server

import akka.actor.{Actor, LoggingFSM}
import ru.ifmo.ctddev.semenov.dkvs.client.RaftClient.RaftResponse
import ru.ifmo.ctddev.semenov.dkvs.log.Log
import ru.ifmo.ctddev.semenov.dkvs.protocol._

import scala.concurrent.duration._

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Node(val id: Int) extends Actor with LoggingFSM[Role, Metadata] {

  import Node._

  startWith(Follower, Metadata(
    id = id,
    journal = new Log(s"dkvs_$id.log"),
    nodes = Array.empty[Metadata.NodeRef] // TODO: resolve nodes
  ))

  resetElectionTimer()

  final val VALUE = "VALUE %s %s"
  final val NOT_FOUND = "NOT_FOUND"
  final val STORED = "STORED"
  final val PONG = "PONG"

  when(Follower) {
    case Event(request: APPEND_ENTRY, meta)                                                         => // TODO: check
      if (meta.term > request.term) {
        sender ! APPEND_ENTRY_RESPONSE(meta.term, success = false)
      } else {
        resetHeartbeatTimer()
        val success = appendEntry(request, meta)
        sender ! APPEND_ENTRY_RESPONSE(meta.term, success)
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
        sender ! APPEND_ENTRY_RESPONSE(meta.term, success = false)
      } else {
        val success = appendEntry(command, meta)
        sender ! APPEND_ENTRY_RESPONSE(meta.term, success)
      }
      stay
    case Event(ElectionTimeout, meta)                          =>
      goto(Candidate) using prepareForCandidate(meta)
  }
  final val DELETED = "DELETED"

  when(Leader) {
    case Event(command: SET, meta)                         =>
      meta put(command, () => sender ! RaftResponse(STORED))
      stay
    case Event(command: DELETE, meta)                      =>
      meta put(command, () => sender ! RaftResponse(DELETED))
      stay
    case Event(SendHeartbeat, meta)                        =>
      meta.nodes foreach (_ ! emptyAppendEntry(meta))
      stay
    case Event(APPEND_ENTRY_RESPONSE(term, success), meta) =>
      if (meta.updateTerm(term)) {
        goto(Follower) using prepareForFollower(meta)
      } else {
        // TODO: update indices
        stay
      }
    case Event(command: REQUEST_VOTE, meta)                =>
      if (meta updateTerm command.term) {
        goto(Follower) using prepareForFollower(meta) replying REQUEST_VOTE_RESPONSE(meta.term, voteGranted = true)
      } else {
        stay replying REQUEST_VOTE_RESPONSE(meta.term, voteGranted = false)
      }
    case Event(_, _)                                       => ??? // TODO: delete
  }

  whenUnhandled {
    case Event(PING, meta) =>
      stay replying RaftResponse(PONG)
    case Event(GET(key), meta) =>
      stay replying RaftResponse(get(key, meta))
  }

  // TODO: better naming
  private def resetElectionTimer() = setTimer(electionTimerName, electionTimerMsg, nextElectionTimeout())

  private def resetHeartbeatTimer() = setTimer(heartbeatTimerName, heartbeatTimerMsg, nextHeartbeatTimeout)

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

  private def emptyAppendEntry(meta: Metadata) =
    APPEND_ENTRY(meta.term, meta.id, meta.journal.lastTerm, meta.journal.lastTerm, null, meta.commitIndex)

  private def get(key: String, meta: Metadata): String = ((meta.map get key) map (VALUE.format(key, _))) getOrElse NOT_FOUND

  private def prepareForCandidate(meta: Metadata): Metadata = {
    meta.nextTerm()
    ??? // TODO
    meta
  }

  private def prepareForLeader(meta: Metadata): Metadata = {
    ??? // TODO
    meta
  }

  private def prepareForFollower(meta: Metadata): Metadata = {
    ??? // TODO
    meta
  }

  initialize() // FSM requirement
}

object Node {
  val electionTimerName = "noHeartbeatTimer"
  val electionTimerMsg = ElectionTimeout
  val heartbeatTimerName = "heartbeatTimerName"
  val heartbeatTimerMsg = SendHeartbeat

  def nextElectionTimeout(): FiniteDuration = (math.random * 100 + 300) millis

  def nextHeartbeatTimeout: FiniteDuration = 150 millis

  // TODO: shouldn't it be in (and extend) Command.scala?
  trait RaftMessages
  case object ElectionTimeout extends RaftMessages
  case object SendHeartbeat extends RaftMessages
}