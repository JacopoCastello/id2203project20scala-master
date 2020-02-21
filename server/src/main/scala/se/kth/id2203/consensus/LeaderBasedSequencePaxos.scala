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

import se.kth.id2203.kvstore.{Op, OperationToPropose}
import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.sics.kompics.KompicsEvent

import scala.collection.mutable

  case class Prepare(nL: (Int,Int) , ld: Int, na: (Int,Int) )extends KompicsEvent;

  case class Promise(nL: (Int,Int), na: (Int,Int), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class AcceptSync(nL: (Int, Int), suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class Accept(nL: (Int, Int), c: RSM_Command) extends KompicsEvent;

  case class Accepted(nL: (Int, Int), m: Int) extends KompicsEvent;

  case class Decide(ld: Int, nL: (Int, Int)) extends KompicsEvent;

  object State extends Enumeration {
    type State = Value;
    val PREPARE, ACCEPT, UNKNOWN = Value;
  }

  object Role extends Enumeration {
    type Role = Value;
    val LEADER, FOLLOWER = Value;
  }



class LeaderBasedSequencePaxos(init: Init[LeaderBasedSequencePaxos]) extends ComponentDefinition {

    import Role._
    import State._

    // reconfig
    var sigma = List.empty[RSM_Command];


  val sc = provides[SequenceConsensus];
    val ble = requires[BallotLeaderElection];
    //val pl: Nothing = requires(FIFOPerfectLink)
    val net = requires[Network]

    val (self, pi, others, c, rself, ri, rothers) = init match {
      case Init(addr: NetAddress,
      pi: Set[NetAddress] @unchecked, // set of processes in config c
      c: Int, // configuration c
      rself: (NetAddress, Int), // Repnumber of this one
      ri:mutable.Map[NetAddress, Int], // set of replicas in config c (ip:port, Repnumber)
      rothers:mutable.Map[NetAddress, Int]) // set of replicas in config c without this one
      => (addr, pi, pi - addr, c, rself, ri, ri-addr)//c = configuration i, ri: RID = Netaddr of process, id
    }
    val majority = (pi.size / 2) + 1;

    var state = (FOLLOWER, UNKNOWN);
  var leader: Option[NetAddress] = None;

  // proposer state
    var nL= (c,0);
  val promises = mutable.Map.empty[Int, ((Int, Int), List[RSM_Command])];
  val las = mutable.Map.empty[NetAddress, Int];
  val lds = mutable.Map.empty[(NetAddress, Int), Int];
  for (p <- pi){
    las += (p -> sigma.size)
  }
  for (r <- ri){
    lds += (r -> 0);
  }
  var propCmds = List.empty[RSM_Command];
  var lc = sigma.size;

  // acceptor state
  var nProm = (c,0);
  var na = (c,0);
  var va = sigma;

  // learner state
  var ld = sigma.size
  // todo: How to compare the SSi and what is SSi??
  var SSi = OperationToPropose(_, Op("STOP", "","",""))

  def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.drop(l)
    }

    def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.take(l)
    }

  // define SSI("Stop", set of ps, ci, set of pids)

  // fun stopped
  def stopped(): Boolean = {
    return (va.last.equals(SSi)); //??
  }

    ble uponEvent { // updated for reconfig
      case BLE_Leader(l, b) => {
        var n = (c , b)
        log.info(s"Proposing leader: $l [$self] (n: $n, nL: $nL)\n")
        /*if (n > nL){
          leader = Some(l);
          nL = n;*/
          if (self == l && (n._1 == nL._1 && n._2 > nL._2)){ // what to compare?
            log.info(s"The leader is host: [$self]\n")
            (nL, nProm) = (n,n)
            state = (LEADER, PREPARE);
            promises += (rself._2 -> (na, suffix(va, ld)))// data structure
            //propCmds = List.empty[RSM_Command];
            for (p <- pi){
              las += (p -> sigma.size);
            }
            lds.clear();
            lds(rself) = ld;
            //promises.clear();
            lc = sigma.size;
            for (r <- rothers){
              trigger(NetMessage(self, r._1, Prepare(nL, ld, na)) -> net);
            }
            //promises(l) = (na, suffix(va, ld));
            //lds(self) = ld;
            //nProm = nL;
          } else{
            state = (FOLLOWER, state._2);
          }
        }
      }

  // upon connection lost
  // upon preparereq

  net uponEvent {
      case NetMessage(p, Prepare(np, ldp, n)) => {
        if (nProm._1 == np._1 && nProm._2 > np._2){
          nProm = np;
          state = (FOLLOWER, PREPARE);
          var sfx = List.empty[RSM_Command];
          if (na._1 == n._1 && na._2 >= n._2 ){
            sfx = suffix(va,ld);
          }
          trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> net);
        }
      }
      case NetMessage(a, Promise(n, na, sfxa, lda)) => { // update reconfig
        if ((n == nL) && (state == (LEADER, PREPARE))) {
          log.info(s"Promise issued with leader: ${a.src}")

          promises(ri(a.src)) = (na, sfxa);
          lds((a.src, ri(a.src))) = lda;
          val P: Set[NetAddress] = pi.filter(x =>  promises(ri(x)) != None);
          if (P.size == (pi.size+1)/2) {
            var ack = P.iterator.reduceLeft((v1, v2) => if (promises(ri(v1))._2.size > promises(ri(v2))._2.size) v1 else v2);
            var (k, sfx) = promises(ri(ack));
            //va = prefix(va, ld) ++ sfx ++ propCmds;
            va = prefix(va, ld) ++ sfx

            if (SSi == va.last) {
              propCmds = List.empty;
            } else if (propCmds.contains(SSi)) { // odering SSi as the last one to add to va
                for (c <- (propCmds - SSi)) {
                  va += c
                }
                va += SSi
              } else {
              for (c <- propCmds) {
                va += c
              }
            }
            las(self) = va.size;
            //propCmds = List.empty;
            state = (LEADER, ACCEPT);

            for (r <- rothers.filter(x => lds(x) != None && lds(x) != va.size)){
              var sfxp = suffix(va,lds(r));
              trigger(NetMessage(self, r._1, AcceptSync(nL, sfxp, lds(r))) -> net);
            }
          }
        } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
          log.info(s"Late request for Promise from: ${a.src}")

          lds((a.src, ri(a.src))) = lda;
          var sfx = suffix(va,lds((a.src, ri(a.src))));
          trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds((a.src, ri(a.src))))) -> net);
          if (lc != sigma.size){ //??
            trigger(NetMessage(self, a.src, Decide(lc, nL)) -> net);
          }
        }
      }
      case NetMessage(p, AcceptSync(nL, sfx, ldp)) => {
        if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
          na = nL;
          va = prefix(va,ldp) ++ sfx;
          trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> net);
          state = (FOLLOWER, ACCEPT);
        }
      }
      case NetMessage(p, Accept(nL, c)) => {
        if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
          va = va ++ List(c);
          trigger(NetMessage(self, p.src, Accepted(nL, va.size)) -> net);
        }
      }
      case NetMessage(_, Decide(l, nL)) => {
        if (nProm == nL){
          while (ld < l){
            trigger(SC_Decide(va(ld)) -> sc);
            ld = ld + 1;
          }
        }
      }
      case NetMessage(a, Accepted(n, la)) => { //changed for reconf
        if ((n == nL) && (state == (LEADER, ACCEPT))) {
          las(a.src) = la;
          var x =  pi.filter(x => las.getOrElse(x, 0) >= la);
          if (lc < la && x.size >= (pi.size+1)/2){
            lc = la;
            for (p <- pi.filter(x => lds(x, c) != None)){ //?????????
              trigger(NetMessage(self, p, Decide(lc, nL)) -> net);
            }
          }
        }
      }

        sc uponEvent { // updated for reconfig
          case SC_Propose(c) => {
            log.info(s"The command {} was proposed!", c)
            log.info(s"The current state of the node is {}", state)
            if (state == (LEADER, PREPARE)) {
              propCmds = propCmds ++ List(c);
            }
            else if (state == (LEADER, ACCEPT) && !stopped()) {
              va = va ++ List(c);
              //las(self) = las(self) + 1;
              las(self) = va.size
              for (r <- rothers.filter(x =>  lds(x) != None)){
                trigger(NetMessage(self, r._1, Accept(nL, c)) -> net);
              }
            }
          }
        }
    }
}
