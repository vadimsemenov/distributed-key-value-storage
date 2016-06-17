package ru.ifmo.ctddev.semenov.dkvs.log

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files._
import java.nio.file.StandardOpenOption._
import java.nio.file.Paths

import scala.collection.mutable
import scala.io.{Codec, Source}

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Log(journalName: String) {
  private val path = Paths.get(journalName)

  private val journal: mutable.Buffer[LogItem] =
    if (exists(path)) Source.fromInputStream(newInputStream(path))(Codec.UTF8).getLines().map(LogItem.apply).toBuffer
    else new scala.collection.mutable.ArrayBuffer[LogItem]()

  private var writer = newBufferedWriter(path, UTF_8, CREATE, WRITE, APPEND)

  def apply(id: Int): Option[LogItem] = if (inRanges(id)) Some(journal(id)) else None

  def inRanges(id: Int) = 0 <= id && id < journal.size

  def +=(logItem: LogItem) = {
    journal += logItem
    writer write logItem.toString
    writer.flush()
  }

  def -=(from: Int) = {
    if (from < journal.size) {
      journal remove(from, journal.size - from)

      // TODO: handle backups
      writer = newBufferedWriter(path, UTF_8, CREATE_NEW, WRITE)

      journal foreach (writer write _.toString)
    }
  }

  def lastTerm = termOf(size - 1)

  def termOf(idx: Int) = if (inRanges(idx)) journal(idx).term else 0

  def lastIndex = journal.size - 1

  def size: Int = journal.size

  override def toString = journal.toString
}
