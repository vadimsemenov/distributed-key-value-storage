package ru.ifmo.ctddev.semenov.dkvs.server

/**
  * @author Vadim Semenov (semenov@rain.ifmo.ru)
  */
sealed trait Role

case object Follower extends Role
case object Candidate extends Role
case object Leader extends Role
