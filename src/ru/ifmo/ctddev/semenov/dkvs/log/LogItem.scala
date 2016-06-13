package ru.ifmo.ctddev.semenov.dkvs.log

import ru.ifmo.ctddev.semenov.dkvs.protocol.{Command, DELETE, SET}
import ru.ifmo.ctddev.semenov.dkvs.DELETE

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class LogItem(term: Int, command: Command) {
  assert(command.isInstanceOf[SET] || command.isInstanceOf[DELETE])

  override def toString = s"$term $command\n"
}

object LogItem {
  def apply(line: String): LogItem = {
    val separator = line.indexOf(' ')
    val term = line.substring(0, separator).toInt
    val command = Command(line.substring(separator + 1))
    new LogItem(term, command)
  }
}
