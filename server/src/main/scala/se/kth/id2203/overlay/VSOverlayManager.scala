/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay;

import se.kth.id2203.bootstrapping._
import se.kth.id2203.networking._
import se.sics.kompics.sl._
import se.sics.kompics.network.Network
import se.sics.kompics.timer.Timer

import util.Random
import se.kth.id2203.failuredetector.{EventuallyPerfectFailureDetector, Restore, Suspect}
import se.kth.id2203.kvstore.Op

/**
  * The V(ery)S(imple)OverlayManager.
  * <p>
  * Keeps all nodes in a single partition in one replication group.
  * <p>
  * Note: This implementation does not fulfill the project task. You have to
  * support multiple partitions!
  * <p>
  * @author Lars Kroll <lkroll@kth.se>
  */
class VSOverlayManager extends ComponentDefinition {

  //******* Ports ******
  val route = provides(Routing);
  val boot = requires(Bootstrapping);
  //val net = requires[Network];
  val net: PositivePort[Network] = requires[Network]
  val timer = requires[Timer];
  val epfd = requires[EventuallyPerfectFailureDetector];

  //// Reconfiguration todo: define a port for starting new replicas or a component for replicas
  val replica =  provides[ReplicaMsg];

  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  private var lut: Option[LookupTable] = None; // --> go to LookupTable
  private var suspected_nodes : Set[NetAddress] = Set();
  //******* Handlers ******
  boot uponEvent {
    case GetInitialAssignments(nodes) => {
      log.info("Generating LookupTable...");
      var lut = LookupTable.generate(nodes, 3);
      logger.debug("Generated assignments:\n$lut");
      trigger(new InitialAssignments(lut) -> boot);
    }
    case Booted(assignment: LookupTable) => {
      log.info("Got NodeAssignment, overlay ready.");
      lut = Some(assignment);
    }
  }

  net uponEvent {
    case NetMessage(header, RouteMsg(key, msg)) => {
      val nodes = lut.get.lookup(key);
      assert(!nodes.isEmpty, "nodes partition is empty");
      nodes.foreach(node => {
      //  if(!suspected_nodes.contains(node)) { // only sent to alive nodes
          trigger(NetMessage(header.src, node, msg) -> net);
          log.info(s"Forwarding message for key $key to $node");
      //  }
      })
    }
    case NetMessage(header, msg: Connect) => {
      lut match {
        case Some(l) => {
          log.debug("Accepting connection request from ${header.src}");
          val size = l.getNodes().size;
          trigger(NetMessage(self, header.src, msg.ack(size)) -> net);
        }
        case None => log.info("Rejecting connection request from ${header.src}, as system is not ready, yet.");
      }
    }

    case NetMessage(header,Suspect(p: NetAddress)) => {
      val nodes = lut.get.getNodes();
      val group = lut.get.getNodesforGroup(p)
      val groupidx = lut.get.getKeyforNode(p)
      if (nodes.contains(p) && !suspected_nodes.contains(p)) {
        log.debug("Suspecting " + p + " creating new replicas")
        // todo: how to start replica for all
        //for (node <- group-p){
          trigger(NetMessage(self, self, BootNewReplica(self, group-p)) -> net);
       // }

        log.debug("Suspecting " + p + " remove from lut")
        lut.get.removeNodefromGroup(p, groupidx)
        suspected_nodes += p
      }

    }

    case NetMessage(header,Restore(p: NetAddress)) => {
      val groupidx = lut.get.getKeyforNode(p)
      if(suspected_nodes.contains(p)){
        log.debug("Restore " + p + "add back to lut")
        suspected_nodes -= p
        // todo: should we put it back?
        lut.get.addNodetoGroup(p,groupidx)

      }

    }
  }

  route uponEvent {
    case RouteMsg(key, msg) => {
      val nodes = lut.get.lookup(key);
      assert(!nodes.isEmpty);
      val i = Random.nextInt(nodes.size);
      val target = nodes.drop(i).head;
      log.info(s"Routing message for key $key to $target");
      trigger(NetMessage(self, target, msg) -> net);
    }
  }


  /*net uponEvent {
    case Suspect(p: NetAddress) => {
      val nodes = lut.get.getNodes();
      val group = lut.get.getNodesforGroup(p)
      val groupidx = lut.get.getKeyforNode(p)
      if (nodes.contains(p) && !suspected_nodes.contains(p)) {
        log.debug("Suspecting " + p + " creating new replicas")
        trigger(BootNewReplica(self, group-p) -> boot); //how do we avoid to start many?
        log.debug("Suspecting " + p + " remove from lut")
        lut.get.removeNodefromGroup(p, groupidx)
        suspected_nodes += p
        //moved to KVParent
     /*   for (node <- group) {
          trigger(NetMessage(self, node, new Op("STOP", "", "", "")) -> net);
        }*/
      }
    }
     case Restore(p:NetAddress) => {
       val groupidx = lut.get.getKeyforNode(p)
      if(suspected_nodes.contains(p)){
        log.debug("Restore " + p + "add back to lut")
        suspected_nodes -= p
        // todo: should we put it back?
        lut.get.addNodetoGroup(p,groupidx)
        /*for (node <- group){
          trigger(NetMessage(self, node, new Op("STOP", "", s"", "")) -> net);
        }*/
      }
    }
  }*/
}
