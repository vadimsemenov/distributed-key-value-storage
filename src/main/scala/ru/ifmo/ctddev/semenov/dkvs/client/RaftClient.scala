package ru.ifmo.ctddev.semenov.dkvs.client

import akka.actor.{Actor, ActorLogging, ActorSelection, ActorSystem, PoisonPill, Props}
import ru.ifmo.ctddev.semenov.dkvs.protocol.Command

import scala.util.{Failure, Success, Try}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class RaftClient(node: ActorSelection) extends Actor with ActorLogging {
  import RaftClient._

  override def receive: Receive = {
    case ConsoleMessage(msg) => Try(Command(msg)) match {
      case Success(command)   => node ! command
      case Failure(exception) => log.error(exception, "illegal command?..")
    }
    case RaftResponse(msg)   =>
      println(msg)
    case Exit                =>
      self ! PoisonPill // graceful stop after processing remaining events
  }
}

object RaftClient {
  sealed trait Message
  case class ConsoleMessage(msg: String) extends Message
  case class RaftResponse(msg: String) extends Message
  case object Exit extends Message

  def props(node: ActorSelection) = Props(classOf[RaftClient], node)

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: RaftClient <host> <port> <id>")
    } else {
      val (host, port, id) = (args(0), args(1), args(2))
      // TODO: check config
      val system = ActorSystem("raft-client")
      // it's irrational, that user should know (host,port,id) of node,
      // it should connect to cluster, not to specific node. isn't it?
      // TODO: add address to config or constant
      // TODO: add option to connect to cluster
      val clusterNode = system.actorSelection(s"akka.tcp://dkvs-system@$host:$port/user/dkvs-$id")
      val client = system.actorOf(props(clusterNode), "raftClient")
      for (line <- scala.io.Source.stdin.getLines()) {
        client ! ConsoleMessage(line.trim)
      }
      client ! Exit
    }
  }
}
