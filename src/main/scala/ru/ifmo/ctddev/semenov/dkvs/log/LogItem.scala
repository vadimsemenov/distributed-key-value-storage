package ru.ifmo.ctddev.semenov.dkvs.log

import ru.ifmo.ctddev.semenov.dkvs.protocol.{Command, DELETE, SET}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
// TODO: another callback on not-stored items
case class LogItem(term: Int, command: Command, onStored: LogItem.onStored) {
  assert(command.isInstanceOf[SET] || command.isInstanceOf[DELETE])

  override def toString = s"$term $command\n"
}

object LogItem {
  type onStored = () => Unit

  val void: onStored = () => Unit

  def apply(line: String): LogItem = {
    if (line == "null") {
      null.asInstanceOf[LogItem]
    } else {
      val separator = line.indexOf(' ')
      val term = line.substring(0, separator).toInt
      val command = Command(line.substring(separator + 1))
      new LogItem(term, command, void)
    }
  }
}
