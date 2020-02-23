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
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.sics.kompics.{KompicsEvent, Start}

import scala.collection.mutable
import se.kth.id2203.kvstore.{Op, OperationToPropose}

  case class Prepare(nL: (Int,Long), ld: Int, na: (Int,Long)) extends KompicsEvent;

  case class Promise(nL: (Int,Long), na: (Int,Long), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class AcceptSync(nL: (Int,Long), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class Accept(nL: (Int,Long), c: RSM_Command) extends KompicsEvent;

  case class Accepted(nL: (Int,Long), m: Int) extends KompicsEvent;

  case class Decide(ld: Int, nL: (Int,Long)) extends KompicsEvent;

  object State extends Enumeration {
    type State = Value;
    val PREPARE, ACCEPT, UNKNOWN, RECOVER = Value;
  }

  object Role extends Enumeration {
    type Role = Value;
    val LEADER, FOLLOWER = Value;
  }



class LeaderBasedSequencePaxos(init: Init[LeaderBasedSequencePaxos]) extends ComponentDefinition {

    import Role._
    import State._

    val sc = provides[SequenceConsensus];
    val ble = requires[BallotLeaderElection];
    val net = requires[Network]

  // initialize:  self, topology, c, (self, c),ri
  val (self, pi, c, rself, ri, rothers, others) = init match {
    case Init(
    addr: NetAddress,
    pi: Set[NetAddress] @unchecked,                     // set of processes in config c
    c: Int,                                              // configuration c
    rself: (NetAddress, Int),                        // Repnumber of this one
    ri:mutable.Map[NetAddress, Int])                   // set of replicas in config c (ip:port, Repnumber
    => (addr, pi, c, rself, ri, ri-addr, pi - addr)   //c = configuration i, ri: RID = Netaddr of process, id
  }


  // reconfig
  var sigma = List.empty[RSM_Command];                // the final sequence from the previous configuration or () if i = 0
  var state = (FOLLOWER, UNKNOWN);
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
  var ld = sigma.size


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
      if (compareGreater(x._1, y._1)) { // suffix with max n
        true
      } else if (x == y && x._2.size > y._2.size) { //longest suffix if equal
        true
      } else {
        false
      }
    }

    // General code
    def stopped(): Boolean = {
      if (va(ld).command.key == "STOP") {
        log.info(s"PAXOS finds STOP in final sequence: \n")
      }
      va(ld).command.key.equals("STOP")
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
          state = (LEADER, PREPARE);
          for (r <- rothers) {
            trigger(NetMessage(self, r._1, Prepare(nL, ld, na)) -> net);
          }
        } else if (state == (state._1, RECOVER)) {
          // not implemented
          // send (PrepareReq) to l
        } else {
          state = (FOLLOWER, state._2);
        }
      }
    }
    // not implemented
    // upon connectionlost
    // upon preparereq


    sc uponEvent {
      case SC_Propose(scp) => {
        log.info(s"The command {} was proposed!", c)
        log.info(s"The current state of the node is {}", state)
        if (state == (LEADER, PREPARE)) {
          propCmds = propCmds ++ List(scp);
        }
        else if (state == (LEADER, ACCEPT) && !stopped()) {
          va = va ++ List(scp);
          las(rself) = va.size;
          for (r <- rothers.filter(x => lds.get(x) != -1)) {
            trigger(NetMessage(self, r._1, Accept(nL, scp)) -> net);
          }
        }
      } //todo: define handover strategy
      /*  case SC_Handover(src,configuration, previous_finalsequence) => { //assuption that new replica already exists
        if(src == self && configuration < c && sigma.isEmpty){
          sigma = previous_finalsequence
           las = mutable.Map.empty[(NetAddress, Int), Int];
            for (r <- ri){
              las += (r -> sigma.size)
            }
            lc = sigma.size;
            va = sigma;
            ld = sigma.size
        }
      } */
    }


    net uponEvent {
      case NetMessage(a, Promise(n, na, sfxa, lda)) => {
        if ((n == nL) && (state == (LEADER, PREPARE))) {
          log.info(s"Promise issued with leader: ${a.src}")
          promises((a.src, ri(a.src))) = (na, sfxa);
          lds((a.src, ri(a.src))) = lda;

          //val P: Set[NetAddress] = pi.filter(x =>  promises((a.src,ri(a.src))) != None);
          val P = pi.filter(x => promises.contains(x, c));
          if (P.size == math.ceil((pi.size + 1) / 2).toInt) {
            var ack = P.iterator.reduceLeft((v1, v2) => if (compareGreaterPromises(promises(v1, ri(v1)), promises(v2, ri(v2)))) v1 else v2);
            var (k, sfx) = promises(ack, ri(ack));
            va = prefix(va, ld) ++ sfx

            // for if below (I don't know how to filter this)
            var comtypes = List.empty[String]
            for (cmd <- propCmds) {
              comtypes = comtypes ++ List(cmd.command.opType)
            }
            if (va.last.command.opType == "STOP") {
              propCmds = List.empty; // commands will never be decided
            } else {
              if (comtypes.contains("STOP")) { // ordering SSi as the last one to add to va
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
              state = (LEADER, ACCEPT);
            }
            for (r <- rothers.filter(x => lds(x) != -1 && lds(x) != va.size)) {
              var sfxp = suffix(va, lds(r));
              trigger(NetMessage(self, r._1, AcceptSync(nL, sfxp, lds(r))) -> net);
            }
          }
        } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
          log.info(s"Late request for Promise from: ${a.src}")
          lds((a.src, ri(a.src))) = lda;
          var sfx = suffix(va, lds((a.src, ri(a.src))));
          trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds((a.src, ri(a.src))))) -> net);
          if (lc != sigma.size) {
            trigger(NetMessage(self, a.src, Decide(lc, nL)) -> net);
          }
        }
      }
      case NetMessage(a, Accepted(n, m)) => {
        if ((n == nL) && (state == (LEADER, ACCEPT))) {
          las((a.src, ri(a.src))) = m;
          var x = pi.filter(x => las.getOrElse((x, c), 0) >= m);
          if (m > lc && x.size >= (pi.size + 1) / 2) {
            lc = m;
            for (p <- others if lds((p, c)) != -1) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> net);
            }
          }
        }
      }
      case NetMessage(p, Prepare(np, ldp, nal)) => {
        if (compareGreater(nProm, np)) {
          nProm = np;
          state = (FOLLOWER, PREPARE);
          var sfx = List.empty[RSM_Command];
          if (compareGreaterEqual(na, nal)) {
            sfx = suffix(va, ldp);
          }
          trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> net);
        }
      }
      case NetMessage(p, AcceptSync(nL, sfx, ldp)) => {
        if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
          na = nL;
          va = prefix(va, ldp) ++ sfx;
          state = (FOLLOWER, ACCEPT);
          trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> net);
        }
      }
      case NetMessage(p, Accept(nL, cmd)) => {
        if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
          va = va ++ List(cmd);
          trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> net);
        }
      }
      case NetMessage(_, Decide(l, nL)) => {
        if (nProm == nL) {
          while (ld < l) {
            trigger(SC_Decide(va(ld)) -> sc);
            ld = ld + 1;
          }
        }
      }
    }
  }

