
package se.kth.id2203.consensus

import se.kth.id2203.networking.NetAddress
import se.sics.kompics.KompicsEvent
import se.sics.kompics.sl._

  case class BLE_Leader(leader: NetAddress, ballot: Long) extends KompicsEvent;

  class BallotLeaderElection extends Port {
    indication[BLE_Leader];
  }
