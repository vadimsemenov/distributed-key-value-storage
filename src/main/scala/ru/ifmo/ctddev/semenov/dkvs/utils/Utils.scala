package ru.ifmo.ctddev.semenov.dkvs.utils

import java.nio.file.{Files, Paths}
import java.util.Properties

import scala.collection.mutable

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
object Utils {
  val NODE_PREFIX = "node."
  val TIMEOUT     = "timeout"

  def readProperties(file: String): (Array[String], Array[Int], Long) = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream(file))
    val hosts = new mutable.ArrayBuffer[String]()
    val ports = new mutable.ArrayBuffer[Int]()
    // 0-indexed
    var id = 0
    var address = properties.getProperty(NODE_PREFIX + id)
    while (address != null) {
      val sep = address.indexOf(':')
      val (host, port) = (address.substring(0, sep), address.substring(sep + 1))
      hosts += host
      ports += port.toInt
      id += 1
      address = properties.getProperty(NODE_PREFIX + id)
    }
    val timeout = properties.getProperty(TIMEOUT, "2000")
    (hosts.toArray, ports.toArray, timeout.toLong)
  }
}
