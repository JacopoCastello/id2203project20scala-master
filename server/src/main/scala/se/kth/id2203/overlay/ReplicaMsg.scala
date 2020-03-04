package se.kth.id2203.overlay


import se.kth.id2203.bootstrapping.NodeAssignment
import se.kth.id2203.consensus.RSM_Command
import se.kth.id2203.networking.NetAddress
import se.sics.kompics.sl._
import se.sics.kompics.KompicsEvent

case class BootNewReplica(sender:NetAddress, nodes: Set[NetAddress], lut: NodeAssignment) extends KompicsEvent;
case class UpdateLookUp(sender:NetAddress, lut: NodeAssignment) extends KompicsEvent;
case class Handover(cOld: Int, sigmaOld:List[RSM_Command]) extends KompicsEvent;

class ReplicaMsg extends Port  {
  indication[BootNewReplica];
  indication[UpdateLookUp];
  indication[Handover];
}
