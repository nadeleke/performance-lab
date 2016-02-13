package com.hastyexperiment

object Util {

  type Time = Double

  type MessageTuple = (MessageType.MessageType, Option[Time], Option[Time], Option[Time], Option[Int])
  type TimeTuple = (Option[Time], Option[Time], Option[Time], Option[Int])

  object MessageType extends Enumeration {
    type MessageType = Value
    val RESULT, EXPERIMENT_DONE = Value

    def fromMessageString(str: String) = str match {
      case "RES" => RESULT
      case "DONE" => EXPERIMENT_DONE
    }
  }
  val redisHost = "ec2-52-34-219-20.us-west-2.compute.amazonaws.com"
  val redisPort = 6379
  val redisKey = "1f56a48f2031433b385483b7566c85f1255af5d3dca24fa378a66645534cf8a7"
}