package ru.ifmo.ctddev.semenov.dkvs

import akka.actor.{ActorSystem, ActorRef, PoisonPill}
import com.typesafe.config.ConfigFactory
import ru.ifmo.ctddev.semenov.dkvs.log.Log
import ru.ifmo.ctddev.semenov.dkvs.server.{Metadata, Node}
import ru.ifmo.ctddev.semenov.dkvs.utils.Utils

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
object Main {
  val author = "СЕМЕНОВ"

  def main(args: Array[String]): Unit = {
    checkVariant(3) // Raft
    val (hosts, ports, timeout) = Utils.readProperties("dkvs.cfg")
    Node.setTimeoutBase(timeout)
    def nodeStartup(id: Int): ActorRef = {
      val log = new Log(s"dkvs_$id.log")
      val config = (ConfigFactory parseString s"akka.remote.netty.tcp.port=${ports(id)}")
        .withFallback(ConfigFactory.load())
      val system = ActorSystem(s"dkvs-system", config)
      val nodes = (for (idx <- hosts.indices) yield
        system.actorSelection(s"akka.tcp://dkvs-system@${hosts(idx)}:${ports(idx)}/user/dkvs-$idx")).toArray
      val meta = Metadata(id, log, nodes)
      system.actorOf(Node.props(meta), s"dkvs-$id")
    }
    if (args.length == 1) {
      val id = args(0).toInt
      val node = nodeStartup(id)
      terminate(node)
    } else {
      val nodes = hosts.indices map (nodeStartup(_))
      terminate(nodes: _*)
    }
  }

  def terminate(nodes: ActorRef*): Unit = {
    scala.io.StdIn.readLine("press return to exit\n")
    nodes foreach (_ ! PoisonPill)
  }

  def checkVariant(variant: Int): Unit = {
    val expected = (author.hashCode & 0x7fffffff) % 3 + 1
    assert(expected == variant, s"You should do ${
      expected match {
        case 1 => "Multi Paxos"
        case 2 => "View Stamped Replication"
        case 3 => "Raft"
        case _ => assert(false)
      }
    }")
  }
}
