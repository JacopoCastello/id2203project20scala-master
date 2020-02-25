package se.kth.id2203

import se.kth.id2203.bootstrapping.{BootNewReplica, Booted, Bootstrapping}
import se.kth.id2203.consensus.{BallotLeaderElection, GossipLeaderElection, LeaderBasedSequencePaxos, SequenceConsensus}
import se.kth.id2203.failuredetector.EPFD
import se.kth.id2203.kvstore.{KVService, Op}
import se.kth.id2203.networking.{NetAddress, NetMessage}
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
    case BootNewReplica(sender: NetAddress, group: Set[NetAddress]) =>{
      print("Boot new replica for node"+ self + " in configuration "+c+" in group "+ group)
      c +=1 // move to next config
      val ri = mutable.Map.empty[NetAddress, Int];
      for (node <- group){
        ri += (node -> c)
      }
      val kv = create(classOf[KVService], Init.NONE) // pass value at handover?
      val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, group, c, (self, c),ri ))
      val gossipLeaderElection = create(classOf[GossipLeaderElection], Init[GossipLeaderElection](self, group))
      val eventuallyPerfectFailureDetector = create(classOf[EPFD], Init[EPFD](self, group))

      // todo: when should they be started?
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



      log.debug("Triggering STOP proposal and start new node"+ self + " in configuration "+c+" in group "+ group )
      for (node <- group) {
        trigger(NetMessage(self, node, new Op("STOP", "", "", "")) -> net);
      }
    }
  }

}