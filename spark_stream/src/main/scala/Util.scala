import java.text.SimpleDateFormat

object Util {

  type Uid = Long
  type Loc = Int
  type Interval = Int
  type Eta = Double

  type MessageTuple1 = (MessageType.MessageType, Option[Loc], Option[Eta])

  type LocEtaPair = (Option[Loc], Option[Eta])
  //  type UserRecord = (Uid, LocEtaPair)
  //  type UidEtaPair = (Uid, Eta)
  //  type EtaList = Seq[Eta]

  val redisHost = "john-redis.2wlafm.ng.0001.usw2.cache.amazonaws.com"
  val redisPort = 6379

  object MessageType extends Enumeration {
    type MessageType = Value
    val RESULT, EXPERIMENT_DONE = Value

    def fromMessageString(str: String) = str match {
      case "RES" => RESULT
      case "DONE" => EXPERIMENT_DONE
    }
  }

}