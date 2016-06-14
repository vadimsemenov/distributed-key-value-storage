package ru.ifmo.ctddev.semenov.dkvs.utils

import java.util.concurrent.{Executors, ScheduledFuture}

import scala.concurrent.duration.TimeUnit

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
class Timer(action: => Any) {
  private val executor = Executors.newSingleThreadScheduledExecutor()
  private var future: ScheduledFuture[_] = null

  def start(timeout: Long, timeUnit: TimeUnit): Unit = future = executor.schedule(new Runnable {
    override def run(): Unit = action
  }, timeout, timeUnit)

  def reset(timeout: Long, timeUnit: TimeUnit): Unit = {
    stop()
    start(timeout, timeUnit)
  }

  def stop(): Unit = if (future != null) future.cancel(true)
}
