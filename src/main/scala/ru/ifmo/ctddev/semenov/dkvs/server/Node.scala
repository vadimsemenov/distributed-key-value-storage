package ru.ifmo.ctddev.semenov.dkvs.server

import java.util.concurrent.{Callable, Executors, TimeUnit}

import ru.ifmo.ctddev.semenov.dkvs.log.Log
import ru.ifmo.ctddev.semenov.dkvs.protocol._
import ru.ifmo.ctddev.semenov.dkvs.utils.{Timer, Utils}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Node(val id: Int) {
  val (portById, timeout) = Utils.readProperties("dkvs.properties")

  val journal = new Log(s"dkvs_$id.log")

  @volatile var role: Role = Follower // all nodes start as follower... todo: check

  var votedFor: Option[Int] = None

  // common fields:
  var commitIndex: Int = 0
  var lastApplied: Int = 0

  // leader fields:
  var nextIndex: Array[Int] = null
  var matchIndex: Array[Int] = null

  var currentLeader: Int = -1
  var currentTerm: Int = 0
  var lastLogIndex: Int = 0
  var lastLogTerm: Int = 0

  val processor = Executors.newSingleThreadExecutor()

  val timer = new Timer {
    processAndGet {
      prepareForCandidate()
    }
  }

  final val VALUE = "VALUE"
  final val NOT_FOUND = "NOT_FOUND"
  final val STORED = "STORED"
  final val DELETED = "DELETED"
  final val PONG = "PONG"

  final val UNEXPECTED_COMMAND = "unexpected command: "

  def get(key: String): String = ???

  def set(key: String, value: String): String = ???

  def delete(key: String): String = ???

  def processCommand(command: Command): String = {
    val future = processor.submit(new Callable[String]() {
      override def call(): String = command match {
        case GET(key)                                                                     =>
          get(key)
        case SET(key, value)                                                              =>
          set(key, value)
        case DELETE(key)                                                                  =>
          delete(key)
        case PING                                                                         =>
          PONG
        case APPEND_ENTRY(term, leaderId, prevLogIndex, prevLogTerm, entry, leaderCommit) =>
          ???
        case APPEND_ENTRY_RESPONSE(term, success)                                         =>
          ???
        case cmd: REQUEST_VOTE                   =>
          val response = requestVote(cmd)
          response.toString
        case REQUEST_VOTE_RESPONSE(term, voteGranted)                                     =>
          ??? // TODO: nothing
        case _                                                                            =>
          UNEXPECTED_COMMAND
      }
    })
    future.get()
  }

  def processAndGet[T](action: => T) = {
    val future = processor.submit(new Callable[T] {
      override def call(): T = action
    })
    future.get()
  }
  def requestVote(request: REQUEST_VOTE): REQUEST_VOTE_RESPONSE = {
    // TODO: check
    if (request.term < currentTerm) {
      REQUEST_VOTE_RESPONSE(currentTerm, voteGranted = false)
    } else {
      currentTerm = request.term
      votedFor match {
        case None | Some(request.candidateId) =>
          if (request.lastLogIndex >= lastLogIndex && request.lastLogTerm >= lastLogTerm) {
            votedFor = Some(request.candidateId)
            REQUEST_VOTE_RESPONSE(currentTerm, voteGranted = true)
          } else {
            REQUEST_VOTE_RESPONSE(currentTerm, voteGranted = false)
          }
        case _                                =>
          REQUEST_VOTE_RESPONSE(currentTerm, voteGranted = false)
      }
    }
  }

  def appendEntry(request: APPEND_ENTRY): APPEND_ENTRY_RESPONSE = {
    // TODO: check
    if (request.term < currentTerm) {
      APPEND_ENTRY_RESPONSE(currentTerm, success = false)
    } else {
      currentTerm = request.term
      journal(request.prevLogIndex) match {
        case None => APPEND_ENTRY_RESPONSE(currentTerm, success = false)
        case Some(item) =>
          if (item.term == request.prevLogTerm) {
            journal -= request.prevLogIndex + 1
            if (request.entry != null) journal += request.entry
            if (request.leaderCommit > commitIndex) commitIndex = Math.min(request.leaderCommit, journal.size)
            APPEND_ENTRY_RESPONSE(currentTerm, success = true)
          } else {
            journal -= request.prevLogIndex
            APPEND_ENTRY_RESPONSE(currentTerm, success = false)
          }
      }
    }
  }

  private def prepareForCandidate() = {
    role = Candidate
    
  }

  private def prepareForLeader() = ???

  private def prepareForFollower() = ???
}

