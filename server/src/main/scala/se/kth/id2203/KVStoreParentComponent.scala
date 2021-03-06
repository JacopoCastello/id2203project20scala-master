package se.kth.id2203

import se.kth.id2203.bootstrapping.{Booted, Bootstrapping}
import se.kth.id2203.consensus.{BallotLeaderElection, GossipLeaderElection, LeaderBasedSequencePaxos, SequenceConsensus}
import se.kth.id2203.failuredetector.EPFD
import se.kth.id2203.kvstore.{KVService, Op}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.kth.id2203.overlay.{BootNewReplica, LookupTable, ReplicaMsg, UpdateLookUp}
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, PositivePort}
import se.sics.kompics.timer.Timer

import scala.collection.mutable

class KVParent extends ComponentDefinition {

  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping)
  val net: PositivePort[Network] = requires[Network]
  val replica: PositivePort[ReplicaMsg] = requires[ReplicaMsg]
  val timer: PositivePort[Timer] = requires[Timer]

 

  val self = cfg.getValue[NetAddress]("id2203.project.address")
  var  c = 0;
  

  boot uponEvent {
    case Booted(assignment: LookupTable) => {

      val topology: Set[NetAddress] = assignment.getNodesforGroup(self)
     

      
      c = 0;
      val ri = mutable.Map.empty[NetAddress, Int];
      for (node <- topology) {
        ri += (node -> c)
      }

      val kv = create(classOf[KVService], Init[KVService](c))
      val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, topology, c, (self, c), ri,  ("FOLLOWER", "UNKNOWN", "RUNNING")))
      
      val gossipLeaderElection = create(classOf[GossipLeaderElection], Init[GossipLeaderElection](self, topology))
      val eventuallyPerfectFailureDetector = create(classOf[EPFD], Init[EPFD](self, topology, c))


      trigger(new Start() -> kv.control())
      trigger(new Start() -> consensus.control())
      trigger(new Start() -> gossipLeaderElection.control())
      trigger(new Start() -> eventuallyPerfectFailureDetector.control())


     
      connect[Timer](timer -> gossipLeaderElection)
      connect[Network](net -> gossipLeaderElection)

     
      connect[BallotLeaderElection](gossipLeaderElection -> consensus)
      connect[Network](net -> consensus)

      
      connect[Network](net -> kv)
      connect[SequenceConsensus](consensus -> kv)

      
      connect[Network](net -> eventuallyPerfectFailureDetector)
      connect[Timer](timer -> eventuallyPerfectFailureDetector)

    }
  }
  net uponEvent {
    case NetMessage(source, BootNewReplica(sender: NetAddress, group: Set[NetAddress], lut: LookupTable)) => {

        log.info("Boot new replica for node" + self + " in configuration " + c + " in group " + group)

        c += 1 
        val ri = mutable.Map.empty[NetAddress, Int];
        for (node <- group) {
          ri += (node -> c)
        }

        val kv = create(classOf[KVService],Init[KVService](c)) // pass value at handover?
        val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, group, c, (self, c), ri, ("FOLLOWER", "UNKNOWN", "WAITING")))
        val gossipLeaderElection = create(classOf[GossipLeaderElection], Init[GossipLeaderElection](self, group))
        val eventuallyPerfectFailureDetector = create(classOf[EPFD], Init[EPFD](self, group, c))

        trigger(new Start() -> kv.control())
        trigger(new Start() -> consensus.control())
        trigger(new Start() -> gossipLeaderElection.control())
        trigger(new Start() -> eventuallyPerfectFailureDetector.control())


        
        connect[Timer](timer -> gossipLeaderElection)
        connect[Network](net -> gossipLeaderElection)

        
        connect[BallotLeaderElection](gossipLeaderElection -> consensus)
        connect[Network](net -> consensus)

       
        connect[Network](net -> kv)
        connect[SequenceConsensus](consensus -> kv)

       
        connect[Network](net -> eventuallyPerfectFailureDetector)
        connect[Timer](timer -> eventuallyPerfectFailureDetector)

        trigger(NetMessage(self, self,UpdateLookUp(self, lut)) -> net);
        log.debug("Triggering STOP proposal and start new node" + self + " in configuration " + c + " in group " + group)

          trigger(NetMessage(self, self, new Op("STOP", "", (c-1).toString, "")) -> net);
        }
      }

}
