package ru.ifmo.ctddev.semenov.dkvs.client

import java.io._
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.LinkedBlockingQueue

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Client(socket: Socket) {
  val consoleReader = new BufferedReader(new InputStreamReader(System.in, UTF_8))
  val consoleWriter = new PrintWriter(new OutputStreamWriter(System.out, UTF_8))
  val socketReader = new BufferedReader(new InputStreamReader(socket.getInputStream, UTF_8))
  val socketWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream, UTF_8), true)

  val requests  = new LinkedBlockingQueue[String]()
  val responses = new LinkedBlockingQueue[String]()

  def start(): Unit = {
    def start(action: () => Unit) = {
      new Thread(new Runnable() {
        override def run() = action()
      }).start()
    }

    start(readRequest)
    start(writeRequest)
    start(readResponse)
    start(writeResponse)
  }

  private def readRequest() = interact(consoleReader.readLine, requests add _, "exit reading requests")
  private def writeRequest() = interact(requests.take, socketWriter.println, "exit writing requests")
  private def readResponse() = interact(socketReader.readLine, responses add _, "exit reading responses")
  private def writeResponse() = interact(responses.take, consoleWriter.println, "exit writing responses")

  @tailrec private def interact(read: () => String, write: String => Unit, exitMessage: String): Unit = {
    val line = read()
    if (line == null) {
      consoleWriter println exitMessage
      return
    }
    write(line)
    interact(read, write, exitMessage)
  }
}

object Connect {
  def main(args: Array[String]) {
    if (args.length != 2) println("Usage: Client <host> <port>")
    else Try(new Socket(args(0), args(1).toInt)) match {
      case Success(socket)    => new Client(socket).start()
      case Failure(exception) =>
        println(s"Cannot connect to ${args(0)}:${args(1)}")
        exception.printStackTrace()
    }
  }
}
