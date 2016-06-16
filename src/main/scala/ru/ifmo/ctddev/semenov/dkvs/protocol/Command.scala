package ru.ifmo.ctddev.semenov.dkvs.protocol

import ru.ifmo.ctddev.semenov.dkvs.log.LogItem

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
trait Command {
  @inline protected final def checkKey(key: String) = require(key.indexOf(' ') == -1, s"key '$key' contains space")
  @inline protected final def checkValue(value: String) = require(value.indexOf('\n') == -1, s"value '$value' contains EOL")
}

object Command {
  def apply(command: String): Command = {
    val separator = command.indexOf(' ')
    if (separator == -1) {
      require(command == PING.toString, s"unknown command '$command'")
      PING
    } else {
      val name = command substring(0, separator)
      val args = (command substring separator + 1) split ","

      name match {
        case "get"                   =>
          require(args.length == 1, s"illegal get arg: '$args'")
          // todo: checkValue(args)?
          GET(args(0))
        case "delete"                =>
          require(args.length == 1, s"illegal delete args: '$args'")
          DELETE(args(0))
        case "set"                   =>
          require(args.length == 2, s"illegal (key,value) pair for set-command: '$args'")
          SET(
            key   = args(0),
            value = args(1)
          )
//        case "appendEntries"         =>
//          require(args.length == 6, s"illegal appendEntries args: '$args'")
//          APPEND_ENTRY(
//            term         = args(0).toInt,
//            leaderId     = args(1).toInt,
//            prevLogIndex = args(2).toInt,
//            prevLogTerm  = args(3).toInt,
//            entry        = LogItem(args(4)),
//            leaderCommit = args(5).toInt)
//        case "appendEntriesResponse" =>
//          require(args.length == 2, s"illegal appendEntriesResponse args: '$args'")
//          APPEND_ENTRY_RESPONSE(
//            term    = args(0).toInt,
//            success = args(1).toBoolean
//          )
//        case "requestVote"           =>
//          require(args.length == 4, s"illegal requestVote args: '$args'")
//          REQUEST_VOTE(
//            term         = args(0).toInt,
//            candidateId  = args(1).toInt,
//            lastLogIndex = args(2).toInt,
//            lastLogTerm  = args(3).toInt
//          )
//        case "requestVoteResponse"   =>
//          require(args.length == 2, s"illegal requestVoteResponse args: '$args'")
//          REQUEST_VOTE_RESPONSE(
//            term        = args(0).toInt,
//            voteGranted = args(1).toBoolean
//          )
//        case "ElectionTimeout"       =>
//        case "SendHeartbeat"         =>
        case _                       =>
          throw new IllegalArgumentException(s"unknown command '$command'")
      }
    }
  }
}

case class GET(key: String) extends Command {
  checkKey(key)
  override def toString = s"get $key"
}

case class SET(key: String, value: String) extends Command{
  checkKey(key)
  checkValue(value)
  override def toString = s"set $key,$value"
}

case class DELETE(key: String) extends Command {
  checkKey(key)
  override def toString = s"delete $key"
}

case object PING extends Command {
  override def toString = "ping"
}

case class APPEND_ENTRY(term: Int, leaderId: Int, prevLogIndex: Int, prevLogTerm: Int, entry: LogItem, leaderCommit: Int) extends Command {
  override def toString = s"appendEntries $term,$leaderId,$prevLogIndex,$prevLogTerm,$entry,$leaderCommit"
}

case class APPEND_ENTRY_RESPONSE(term: Int, id: Int, success: Boolean) extends Command {
  override def toString = s"appendEntriesResponse $term,$success"
}

case class REQUEST_VOTE(term: Int, candidateId: Int, lastLogIndex: Int, lastLogTerm: Int) extends Command {
  override def toString = s"requestVote $term,$candidateId,$lastLogIndex,$lastLogTerm"
}

case class REQUEST_VOTE_RESPONSE(term: Int, voteGranted: Boolean) extends Command {
  override def toString = s"requestVote $term,$voteGranted"
}

trait RaftCommand extends Command
case object ElectionTimeout extends RaftCommand
case object SendHeartbeat extends RaftCommand