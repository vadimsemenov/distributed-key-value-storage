package ru.ifmo.ctddev.semenov.dkvs.server

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
trait Condition

case object Follower extends Condition
case object Candidate extends Condition
case object Leader extends Condition
