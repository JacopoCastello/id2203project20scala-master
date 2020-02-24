package se.kth.id2203

import se.kth.id2203.bootstrapping.{BootNewReplica, Booted, Bootstrapping}
import se.kth.id2203.consensus.{BallotLeaderElection, GossipLeaderElection, LeaderBasedSequencePaxos, SequenceConsensus}
import se.kth.id2203.failuredetector.EPFD
import se.kth.id2203.kvstore.KVService
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, PositivePort}
import se.sics.kompics.timer.Timer

import scala.collection.mutable

class KVParent extends ComponentDefinition {

  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping)
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]

  val self = cfg.getValue[NetAddress]("id2203.project.address")
  var  c = 0;
  // kv = create(classOf[KVService], Init.NONE)

  boot uponEvent {
    case Booted(assignment: LookupTable) => {

      val topology: Set[NetAddress] = assignment.getNodesforGroup(self)
      //val topology = assignment.getNodes()

      val kv = create(classOf[KVService], Init.NONE)
      // initial creation configuration 0. At the new configuration, the KVStoreParent should be passed the id
      c = 0;
      val ri = mutable.Map.empty[NetAddress, Int];
      for (node <- topology){
        ri += (node -> c)
      }
      val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, topology, c, (self, c),ri ))
      //val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, topology ))
      val gossipLeaderElection = create(classOf[GossipLeaderElection], Init[GossipLeaderElection](self, topology))
      val eventuallyPerfectFailureDetector = create(classOf[EPFD], Init[EPFD](self, topology))


      trigger(new Start() -> kv.control())
      trigger(new Start() -> consensus.control())
      trigger(new Start() -> gossipLeaderElection.control())
      trigger(new Start() -> eventuallyPerfectFailureDetector.control())


      // BallotLeaderElection (for paxos)
      connect[Timer](timer -> gossipLeaderElection)
      connect[Network](net -> gossipLeaderElection)

      // Paxos
      connect[BallotLeaderElection](gossipLeaderElection -> consensus)
      connect[Network](net -> consensus)

      // KV
      connect[Network](net -> kv)
      connect[SequenceConsensus](consensus -> kv)

      //EPFD
      connect[Network](net -> eventuallyPerfectFailureDetector)
      connect[Timer](timer -> eventuallyPerfectFailureDetector)

    }
    case BootNewReplica(sender: NetAddress, nodes: Set[NetAddress]) =>{
      print("boot new replicas in config: $c ")
    }
  }

}