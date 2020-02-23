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
import se.sics.kompics.KompicsEvent

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
    //val pl: Nothing = requires(FIFOPerfectLink)
    val net = requires[Network]

  // initialize
  val (self, pi, c, rself, ri, rothers, others) = init match {
    case Init(addr: NetAddress,
    pi: Set[NetAddress] @unchecked, // set of processes in config c
    c: Int, // configuration c
    rself: (NetAddress, Int), // Repnumber of this one
    ri:mutable.Map[NetAddress, Int]) // set of replicas in config c (ip:port, Repnumber
    => (addr, pi, c, rself, ri, ri-addr, pi - addr)//c = configuration i, ri: RID = Netaddr of process, id
  }
    //val majority = (pi.size / 2) + 1;

  // reconfig
  var sigma = List.empty[RSM_Command]; // the final sequence from the previous configuration or hi if   i = 0
  var state = (FOLLOWER, UNKNOWN);

  var leader: Option[NetAddress] = None;

  // proposer state
  var nL= (c,0l); //var nL = 0l;
  var promises = mutable.Map.empty[(NetAddress, Int), ((Int, Long), List[RSM_Command])]; //val acks = mutable.Map.empty[NetAddress, (Long, List[RSM_Command])];

  val las = mutable.Map.empty[(NetAddress, Int), Int]; //length of longest accepted sequence per acceptor
  val lds = mutable.Map.empty[(NetAddress, Int), Int]; // length of longest known decided sequence per acceptor
  for (r <- ri){
    las += (r -> sigma.size)
    lds += (r -> 0);
  }
  var propCmds = List.empty[RSM_Command]; //set of commands that need to be appended to the log
  var lc = sigma.size; //length of longest chosen sequence *


  // acceptor state
  var nProm = (c,0l); //var nProm = 0l;
  var na = (c,0l); //var na = 0l;
  var va = sigma; //var va = List.empty[RSM_Command];


  // learner state
  var ld = sigma.size  //var ld = 0;


    def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.drop(l)
    }

    def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.take(l)
    }
    def stopped(): Boolean = {
      if (va(ld).command.key.equals("STOP")) {
        log.info(s"PAXOS finds STOP in final sequence: \n")
      }
      va(ld).command.key.equals("STOP")
    }
  def compareGreater(x: (Int, Long), y:(Int,Long)): Boolean ={
    if((x._1 > y._1) || (x._1 == y._1 && x._2 > y._2)){
       true
    }else{
       false
    }
  }
  def compareGreaterEqual(x: (Int, Long), y:(Int,Long)): Boolean ={
    if((x._1 >= y._1) || (x._1 == y._1 && x._2 >= y._2)){
      true
    }else{
      false
    }
  }


    ble uponEvent {
      case BLE_Leader(l, b) => {
        log.info(s"Proposing leader: $l [$self] \n")
        println(nL)
        var n = (c,b)
        if (self.equals(l) && compareGreater(n,nL)){
          //leader = Some(l);
          //nL = n;
          //if (self == l && nL > nProm){
          log.info(s"The leader is host: [$self]\n")
          nL = n
          nProm = n
          promises = scala.collection.mutable.Map(rself -> (na, suffix(va, ld)))   // data structure
          //propCmds = List.empty[RSM_Command];
          for (r <- ri){
            las += (r -> sigma.size);
            lds += (r -> 0);
          }
          lds(rself) = ld;
          lc = sigma.size;
          state = (LEADER, PREPARE);
          for (r <- rothers){
            trigger(NetMessage(self, r._1, Prepare(nL, ld, na)) -> net);
          }
        } else if(state == (state._1, RECOVER)) {
        }else{
          state = (FOLLOWER, state._2);
        }
      }
    }
  // upon connectionlost
  // upon preparereq

    net uponEvent {
      case NetMessage(p, Prepare(np, ldp, n)) => {
        if (compareGreater(nProm, np)) {
          nProm = np;
          state = (FOLLOWER, PREPARE);
          var sfx = List.empty[RSM_Command];
          if (compareGreaterEqual(na, n)) {
            sfx = suffix(va, ld);
          }
          trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> net);
        }
      }
      case NetMessage(a, Promise(n, na, sfxa, lda)) => {
        if ((n == nL) && (state == (LEADER, PREPARE))) {
          log.info(s"Promise issued with leader: ${a.src}")
          promises((a.src, ri(a.src))) = (na, sfxa);
          lds((a.src, ri(a.src))) = lda;

          //val P: Set[NetAddress] = pi.filter(x =>  promises((a.src,ri(a.src))) != None);
          val P = pi.filter(x => promises.contains(x, c));
          if (P.size == (pi.size + 1) / 2) {
            var ack = P.iterator.reduceLeft((v1, v2) => if (promises(v1,ri(v1))._2.size > promises(v2,ri(v2))._2.size) v1 else v2);
            var (k, sfx) = promises(ack,ri(ack));
            //var (k, sfx) = promises(P.maxBy(item => promises(item,c)._2.size)); //what to compare in acks??
            //val sfx = promises.values.maxBy(_._1)._2
            //va = prefix(va, ld) ++ sfx ++ propCmds;
            va = prefix(va, ld) ++ sfx

            if (va.last.command == "STOP") {
              propCmds = List.empty; //* commands will never be decided
            } else if (propCmds.contains(Op("STOP", "", "", ""))) { // ordering SSi as the last one to add to va
              var stop = propCmds.filter(x => x.command == "STOP")
              propCmds = propCmds.filter(x => x.command != "STOP")

              for (c <- propCmds) {
                va = va ++ List(c)
              }
              va = va ++ stop
            } else {
              for (c <- propCmds) {
                va = va ++ List(c)
              }
            }
            las((self, c)) = va.size;
            //propCmds = List.empty;
            state = (LEADER, ACCEPT);

            for (r <- rothers.filter(x => lds(x) != 0 && lds(x) != va.size)) {
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
      case NetMessage(p, AcceptSync(nL, sfx, ldp)) => {
        if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
          na = nL;
          va = prefix(va, ldp) ++ sfx;
          state = (FOLLOWER, ACCEPT);
          trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> net);
        }
      }
      case NetMessage(p, Accept(nL, c)) => {
        if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
          va = va ++ List(c);
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
      case NetMessage(a, Accepted(n, m)) => {
        if ((n == nL) && (state == (LEADER, ACCEPT))) {
          las((a.src, ri(a.src))) = m;
          var x = pi.filter(x => las.getOrElse((x, c), 0) >= m);
          if (lc < m && x.size >= (pi.size + 1) / 2) {
            lc = m;
            for (p <- pi if lds.contains((p, c))) {
              trigger(NetMessage(self, p, Decide(lc, nL)) -> net);
            }
          }
        }
      }


        sc uponEvent {
          case SC_Propose(c) => {
            log.info(s"The command {} was proposed!", c)
            log.info(s"The current state of the node is {}", state)
            if (state == (LEADER, PREPARE)) {
              propCmds = propCmds ++ List(c);
            }
            else if (state == (LEADER, ACCEPT) && !stopped()) {
              va = va ++ List(c);
              las(rself) = va.size;
              for (r <- rothers.filter(x =>  lds.get(x) != 0)){
                trigger(NetMessage(self, r._1, Accept(nL, c)) -> net);
              }
            }
          }
        }
    }
}
