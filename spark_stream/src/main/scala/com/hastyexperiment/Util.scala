package com.hastyexperiment

object Util {

  type Time = Double

  type MessageTuple = (MessageType.MessageType, Option[Time])

  object MessageType extends Enumeration {
    type MessageType = Value
    val RESULT, EXPERIMENT_DONE = Value

    def fromMessageString(str: String) = str match {
      case "RES" => RESULT
      case "DONE" => EXPERIMENT_DONE
    }
  }

}