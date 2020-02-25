package se.kth.id2203.overlay


import se.kth.id2203.networking.NetAddress
import se.sics.kompics.sl._
import se.sics.kompics.KompicsEvent

case class BootNewReplica(sender:NetAddress, nodes: Set[NetAddress]) extends KompicsEvent;

class ReplicaMsg extends Port  {
  indication[BootNewReplica];
}
