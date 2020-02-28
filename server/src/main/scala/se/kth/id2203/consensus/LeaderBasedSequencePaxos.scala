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
package se.kth.id2203.consensus

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.KompicsEvent
import se.sics.kompics.network._
import se.sics.kompics.sl._

import scala.collection.mutable

  case class Prepare(nL: (Int,Long), ld: Int, na: (Int,Long)) extends KompicsEvent;

  case class Promise(nL: (Int,Long), na: (Int,Long), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class AcceptSync(nL: (Int,Long), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class Accept(nL: (Int,Long), c: RSM_Command) extends KompicsEvent;

  case class Accepted(nL: (Int,Long), m: Int, conf: Int) extends KompicsEvent;

  case class Decide(ld: Int, nL: (Int,Long)) extends KompicsEvent;

/*  object State extends Enumeration {
    type State = Value;
    val PREPARE, ACCEPT, UNKNOWN, RECOVER = Value;
  }

  object Role extends Enumeration {
    type Role = Value;
    val LEADER, FOLLOWER = Value;
  }

object ReconfigurationState extends Enumeration {
  type ReconfigurationState = Value;
  val WAITING, RUNNING, HELPING = Value;
}*/

case class SC_Handover(cOld: Int, sigmaOld:List[RSM_Command]) extends KompicsEvent;

class LeaderBasedSequencePaxos(init: Init[LeaderBasedSequencePaxos]) extends ComponentDefinition {

    //import Role._
    //import State._

    val sc = provides[SequenceConsensus];
    val ble = requires[BallotLeaderElection];
    val net = requires[Network]

  // initialize:  self, topology, c, (self, c),ri
  var (self, pi, c, rself, ri, state, rothers, others) = init match {
    case Init(
    addr: NetAddress,
    pi: Set[NetAddress] @unchecked,                     // set of processes in config c
    c: Int,                                              // configuration c
    rself: (NetAddress, Int),                        // Repnumber of this one
    ri:mutable.Map[NetAddress, Int],
    state:(String,String,String))                   // set of replicas in config c (ip:port, Repnumber
    => (addr, pi, c, rself, ri, state, ri - addr, pi - addr)   //c = configuration i, ri: RID = Netaddr of process, id
  }


  // reconfig
  var sigma = List.empty[RSM_Command];                // the final sequence from the previous configuration or () if i = 0
  //var state = (FOLLOWER, UNKNOWN);
  //var leader: Option[NetAddress] = None;

  // proposer state
  var nL= (c,0l); //var nL = 0l;
  var promises = mutable.Map.empty[(NetAddress, Int), ((Int, Long), List[RSM_Command])]; //val acks = mutable.Map.empty[NetAddress, (Long, List[RSM_Command])];
  val las = mutable.Map.empty[(NetAddress, Int), Int];                                    //length of longest accepted sequence per acceptor
  val lds = mutable.Map.empty[(NetAddress, Int), Int];                                    // length of longest known decided sequence per acceptor
  for (r <- ri){
    las += (r -> sigma.size)
    lds += (r -> -1);
  }
  var propCmds = List.empty[RSM_Command];                                                 //set of commands that need to be appended to the log
  var lc = sigma.size;                                                                    //length of longest chosen sequence


  // acceptor state
  var nProm = (c,0l);
  var na = (c,0l);
  var va = sigma;


  // learner state
  var   ld = sigma.size

    def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.drop(l)
    }

    def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.take(l)
    }

  def compareGreater(x: (Int, Long), y:(Int,Long)): Boolean ={
    if((x._1 > y._1) || (x._1 == y._1 && x._2 > y._2)){
       true
    }else{
       false
    }
  }

  def compareGreaterEqual(x: (Int, Long), y:(Int,Long)): Boolean = {
    if ((x._1 >= y._1) || (x._1 == y._1 && x._2 >= y._2)) {
      true
    } else {
      false
    }
  }

    def compareGreaterPromises(x: ((Int, Long), List[RSM_Command]), y: ((Int, Long), List[RSM_Command])): Boolean = {
      if (compareGreater(x._1, y._1) || (x._1._1 == y._1._1 && x._1._2 == y._1._2 && x._2.size > y._2.size)) { // suffix with max n
        true
      } else {
        false
      }
    }

    // General code
    def stopped(): Boolean = {
      if (!va.isEmpty) {
        if(va.last.command.opType == "STOP" && va.last.command.value == c.toString()) {
          log.info(s"PAXOS finds STOP in final sequence: \n")
          true
        }
      }
      false
    }



    ble uponEvent {
      case BLE_Leader(l, b) => {
        log.info(s"Proposing leader: $l [$self] \n")
        println(nL)
        var n = (c, b)
        if (self == l && compareGreater(n, nL)) {
          log.info(s"The leader is host: [$self]\n")
          nL = n
          nProm = n
          promises = scala.collection.mutable.Map(rself -> (na, suffix(va, ld)))
          for (r <- ri) {
            las += (r -> sigma.size);
            lds += (r -> -1);
          }
          lds(rself) = ld;
          lc = sigma.size;
          state = ("LEADER", "PREPARE", "RUNNING");
          for (r <- rothers) {
            trigger(NetMessage(self, r._1, Prepare(nL, ld, na)) -> net);
          }
          // not implemented
      //  } else if (state == (state._1, RECOVER)) {
          // send (PrepareReq) to l
        } else {
          state = ("FOLLOWER", state._2, "RUNNING");
        }

      }
    }
    // not implemented:
    // upon connectionlost
    // upon preparereq

    // Consensus
    sc uponEvent {
      case SC_Propose(scp) => {
        log.info(s"The command {} was proposed!", scp.command.opType)
        log.info(s"The current state of the node is {}", state)
        if (state == ("LEADER", "PREPARE", "RUNNING")) {
          propCmds = propCmds ++ List(scp);
        }
        else if (state == ("LEADER", "ACCEPT", "RUNNING") && !stopped()) {
          va = va ++ List(scp);
          las(rself) = va.size;
          for (r <- rothers.filter(x => lds.get(x) != -1)) {
            trigger(NetMessage(self, r._1, Accept(nL, scp)) -> net);
          }
        }
      }
    }


    net uponEvent {
      case NetMessage(sender, SC_Handover(cOld, sigmaOld)) => {
        if(sender.src == self && cOld == c-1 && sigmaOld.last.command.opType == "STOP"){
          log.info("Handover")
            sigma = sigmaOld
            las.clear()
            for (r <- ri){
              las += (r -> sigma.size)
            }
            lc = sigma.size;
            va = sigma;
            ld = sigma.size
            state = (state._1, state._2, "RUNNING")
        }
      }
      case NetMessage(a, Promise(n, na, sfxa, lda)) => {
        log.info(s"Value of p: ${a.src}")
        log.info(s"Value of np: ${n}")
        if ((n == nL) && (state == ("LEADER", "PREPARE", "RUNNING"))) {
          log.info(s"Promise issued with leader: ${a.src}")
          promises((a.src, ri(a.src))) = (na, sfxa);
          lds((a.src, ri(a.src))) = lda;

          val P = pi.filter(x => promises.contains(x, ri(a.src)));
          if (P.size == math.ceil((pi.size + 1) / 2).toInt) {
            var ack = P.iterator.reduceLeft((v1, v2) => if (compareGreaterPromises(promises(v1, ri(v1)), promises(v2, ri(v2)))) v1 else v2);
            var (k, sfx) = promises((ack,c));
            va = prefix(va, ld) ++ sfx

            var comtypes = List.empty[String]
            for (cmd <- propCmds) {
              comtypes = comtypes ++ List(cmd.command.opType)
            }
            if (!va.isEmpty && va.last.command.opType == "STOP" && va.last.command.value.toInt == c) {
              propCmds = List.empty; // commands will never be decided
            } else {
              if (comtypes.contains("STOP") && va.last.command.value.toInt == c) { // ordering SSi as the last one to add to va
                var stop = propCmds.filter(x => x.command.opType == "STOP")
                for (cmd <- propCmds.filter(x => x.command.opType != "STOP")) {
                  va = va ++ List(cmd)
                }
                va = va ++ stop
              } else {
                for (cmd <- propCmds) {
                  va = va ++ List(cmd)
                }
              }
              las((self, c)) = va.size;
              state = ("LEADER", "ACCEPT","RUNNING");
            }
            for (r <- rothers.filter(x => lds(x) != -1 )) {//&& lds(x) != va.size
              var sfxp = suffix(va, lds(r))
              trigger(NetMessage(self, r._1, AcceptSync(nL, sfxp, lds(r))) -> net);
            }
          }
        } else if ((n == nL) && (state == ("LEADER", "ACCEPT","RUNNING"))) {
          log.info(s"Late request for Promise from: ${a.src}")
          lds((a.src, ri(a.src))) = lda;
          var sfx = suffix(va, lds((a.src, ri(a.src))));
          trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds((a.src, ri(a.src))))) -> net);
          if (lc != sigma.size) {
            trigger(NetMessage(self, a.src, Decide(lc, nL)) -> net);
          }
        }
      }
      case NetMessage(a, Accepted(n, m, conf)) => {
        log.info("The received sequence has length: " + m)
        if ((conf == c) && (n == nL) && (state == ("LEADER", "ACCEPT", "RUNNING"))) {
          las((a.src, ri(a.src))) = m;
          var x = pi.filter(x => las.getOrElse((x, rself._2), 0) >= m);
          if (m > lc && x.size >= (pi.size + 1) / 2) {
            lc = m;
            for (p <- pi if lds((p, c)) != -1) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> net);
            }
          }
        }
      }
      case NetMessage(p, Prepare(np, ldp, nal)) => {
        log.info(s"Value of p: ${p.src}")
        log.info(s"Value of np: ${np}")
       // if (compareGreater(nProm, np)) {
        if (compareGreater(np, nProm)) {
          nProm = np;
          state = ("FOLLOWER", "PREPARE", "RUNNING");
          var sfx = List.empty[RSM_Command];
          if (compareGreaterEqual(na, nal)) {
            sfx = suffix(va, ldp);
          }
          trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> net);
        }
      }
      case NetMessage(p, AcceptSync(localnL, sfx, ldp)) => {
        if ((nProm == localnL) && (state == ("FOLLOWER", "PREPARE", "RUNNING"))) {
          na = localnL;
          va = prefix(va, ldp) ++ sfx;
          state = ("FOLLOWER", "ACCEPT","RUNNING");
          trigger(NetMessage(self, p.src, Accepted(localnL, va.size, c)) -> net);
        }
      }
      case NetMessage(p, Accept(localnL, cmd)) => {
        if ((nProm == localnL) && (state == ("FOLLOWER", "ACCEPT", "RUNNING"))) {
          va = va ++ List(cmd);
          trigger(NetMessage(self, p.src, Accepted(localnL, va.size, c)) -> net);
        }
      }
      case NetMessage(_, Decide(l, localnL)) => {
        if (nProm == localnL) {
          while (ld < l) {
            if (va(ld).command.opType == "STOP" && va(ld).command.value == c.toString){
              state = (state._1, state._2, "HELPING");
              trigger(NetMessage(self, self, SC_Handover(c, va)) -> net)
              suicide()
              // KIll the components - triger kill on component
            }
            trigger(SC_Decide(va(ld)) -> sc);
            ld = ld + 1;
          }
        }
      }
    }
  }

