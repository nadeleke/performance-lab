package com.hastyexperiment

object Util {

  type Time = Double

  type MessageTuple = (MessageType.MessageType, Option[Time], Option[Time], Option[Time])
  type TimeTuple = (Option[Time], Option[Time], Option[Time])

  object MessageType extends Enumeration {
    type MessageType = Value
    val RESULT, EXPERIMENT_DONE = Value

    def fromMessageString(str: String) = str match {
      case "RES" => RESULT
      case "DONE" => EXPERIMENT_DONE
    }
  }
  val redisHost = "ec2-52-89-35-171.us-west-2.compute.amazonaws.com"
  val redisPort = 6379
  val webdisPort = 7379

}