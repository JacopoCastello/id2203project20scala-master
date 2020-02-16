package se.kth.id2203

import se.kth.id2203.bootstrapping.{Booted, Bootstrapping}
import se.kth.id2203.consensus.{BallotLeaderElection, GossipLeaderElection, LeaderBasedSequencePaxos, SequenceConsensus}
import se.kth.id2203.kvstore.KVService
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.overlay.LookupTable
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, PositivePort}
import se.sics.kompics.timer.Timer

class KVParent extends ComponentDefinition {

  val boot: PositivePort[Bootstrapping.type] = requires(Bootstrapping)
  val net: PositivePort[Network] = requires[Network]
  val timer: PositivePort[Timer] = requires[Timer]

  boot uponEvent {
    case Booted(assignment: LookupTable) => {
      val self = cfg.getValue[NetAddress]("id2203.project.address")
      //val topology: Set[NetAddress] = assignment.lookupSelf(self)
      val topology = assignment.getNodes()

      val kv = create(classOf[KVService], Init.NONE)
      val consensus = create(classOf[LeaderBasedSequencePaxos], Init[LeaderBasedSequencePaxos](self, topology))
      val gossipLeaderElection = create(classOf[GossipLeaderElection], Init[GossipLeaderElection](self, topology))

      trigger(new Start() -> kv.control())
      trigger(new Start() -> consensus.control())
      trigger(new Start() -> gossipLeaderElection.control())

      // BallotLeaderElection (for paxos)
      connect[Timer](timer -> gossipLeaderElection)
      connect[Network](net -> gossipLeaderElection)

      // Paxos
      connect[BallotLeaderElection](gossipLeaderElection -> consensus)
      connect[Network](net -> consensus)

      // KV (the actual thing)
      connect[Network](net -> kv)
      connect[SequenceConsensus](consensus -> kv)
    }
  }
}