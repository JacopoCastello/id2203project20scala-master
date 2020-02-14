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

  case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;

  case class Promise(nL: Long, na: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class AcceptSync(nL: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;

  case class Accept(nL: Long, c: RSM_Command) extends KompicsEvent;

  case class Accepted(nL: Long, m: Int) extends KompicsEvent;

  case class Decide(ld: Int, nL: Long) extends KompicsEvent;

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

    val sc = provides[SequenceConsensus];
    val ble = requires[BallotLeaderElection];
    //val pl: Nothing = requires(FIFOPerfectLink)
    val net = requires[Network]

    val (self, pi, others) = init match {
      case Init(addr: NetAddress, pi: Set[NetAddress] @unchecked) => (addr, pi, pi - addr)
    }
    val majority = (pi.size / 2) + 1;

    var state = (FOLLOWER, UNKNOWN);
    var nL = 0l;
    var nProm = 0l;
    var leader: Option[NetAddress] = None;
    var na = 0l;
    var va = List.empty[RSM_Command];
    var ld = 0;
    // leader state
    var propCmds = List.empty[RSM_Command];
    val las = mutable.Map.empty[NetAddress, Int];
    val lds = mutable.Map.empty[NetAddress, Int];
    var lc = 0;
    val acks = mutable.Map.empty[NetAddress, (Long, List[RSM_Command])];

    def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.drop(l)
    }

    def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
      s.take(l)
    }

    ble uponEvent {
      case BLE_Leader(l, n) => {
        log.info(s"Proposing leader: $l [$self] (n: $n, nL: $nL)\n")
        if (n > nL){
          leader = Some(l);
          nL = n;
          if (self == l && nL > nProm){
            log.info(s"The leader is host: [$self]\n")
            state = (LEADER, PREPARE);
            propCmds = List.empty[RSM_Command];
            las.clear();
            lds.clear();
            acks.clear();
            lc = 0;
            for (p <- others){
              trigger(NetMessage(self, p, Prepare(nL, ld, na)) -> net);
            }
            acks(l) = (na, suffix(va, ld));
            lds(self) = ld;
            nProm = nL;
          } else{
            state = (FOLLOWER, state._2);
          }
        }
      }
    }

    net uponEvent {
      case NetMessage(p, Prepare(np, ldp, n)) => {
        if (nProm < np){
          nProm = np;
          state = (FOLLOWER, PREPARE);
          var sfx = List.empty[RSM_Command];
          if (na >= n){
            sfx = suffix(va,ld);
          }
          trigger(NetMessage(self, p.src, Promise(np, na, sfx, ld)) -> net);
        }
      }
      case NetMessage(a, Promise(n, na, sfxa, lda)) => {
        if ((n == nL) && (state == (LEADER, PREPARE))) {
          log.info(s"Promise issued with leader: ${a.src}")

          acks(a.src) = (na, sfxa);
          lds(a.src) = lda;
          val P: Set[NetAddress] = pi.filter(x => acks.get(x) != None);
          if (P.size == (pi.size+1)/2) {
            var ack = P.iterator.reduceLeft((v1,v2) => if (acks(v1)._2.size > acks(v2)._2.size) v1 else v2);
            var (k, sfx) = acks(ack);
            va = prefix(va, ld) ++ sfx ++ propCmds;
            las(self) = va.size;
            propCmds = List.empty;
            state = (LEADER, ACCEPT);
            for (p <- others.filter(x => lds.get(x) != None)){
              var sfxp = suffix(va,lds(p));
              trigger(NetMessage(self, p, AcceptSync(nL, sfxp, lds(p))) -> net);
            }
          }
        } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
          log.info(s"Late request for Promise from: ${a.src}")

          lds(a.src) = lda;
          var sfx = suffix(va,lds(a.src));
          trigger(NetMessage(self, a.src, AcceptSync(nL, sfx, lds(a.src))) -> net);
          if (lc != 0){
            trigger(NetMessage(self, a.src, Decide(ld, nL)) -> net);
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
      case NetMessage(a, Accepted(n, m)) => {
        if ((n == nL) && (state == (LEADER, ACCEPT))) {
          las(a.src) = m;
          var x =  pi.filter(x => las.getOrElse(x, 0) >= m);
          if (lc < m && x.size >= (pi.size+1)/2){
            lc = m;
            for (p <- pi.filter(x => lds.get(x) != None)){
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
            else if (state == (LEADER, ACCEPT)) {
              va = va ++ List(c);
              las(self) = las(self) + 1;
              for (p <- others.filter(x =>  lds.get(x) != None)){
                trigger(NetMessage(self, p, Accept(nL, c)) -> net);
              }
            }
          }
        }
    }
}
