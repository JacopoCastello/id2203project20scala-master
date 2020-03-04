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
import se.sics.kompics.network._
import se.sics.kompics.sl._
import se.sics.kompics.timer.{ScheduleTimeout, Timeout, Timer}
import se.sics.kompics.{KompicsEvent, Start}

import scala.collection.mutable

case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

case class HeartbeatReq(round: Long, highestBallot: Long) extends KompicsEvent;

case class HeartbeatResp(round: Long, ballot: Long) extends KompicsEvent;

class GossipLeaderElection(init: Init[GossipLeaderElection]) extends ComponentDefinition {

  val ble = provides[BallotLeaderElection];
  val timer = requires[Timer];
  val net = requires[Network]

  val (self, topology, configN) = init match {
    case Init(self: NetAddress, topology: Set[NetAddress], configN: Int) => (self, topology, configN)
  }

  val delta = 10;
  val majority = (topology.size / 2) + 1;

  private var period = 100;
  private val ballots = mutable.Map.empty[NetAddress, Long];

  private var round = 0l;
  private var ballot = ballotFromNAddress(0, self);

  private var leader: Option[(Long, NetAddress)] = None;
  private var highestBallot: Long = ballot;
  private val ballotOne = 0x0100000000l;

  private def startTimer(delay: Long): Unit = {
    val scheduledTimeout = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  def ballotFromNAddress(n: Int, adr: Address): Long = {
    val nBytes = com.google.common.primitives.Ints.toByteArray(n);
    val addrBytes = com.google.common.primitives.Ints.toByteArray(adr.hashCode());
    val bytes = nBytes ++ addrBytes;
    val r = com.google.common.primitives.Longs.fromByteArray(bytes);
    assert(r > 0); // should not produce negative numbers!
    r
  }

  def incrementBallotBy(ballot: Long, inc: Int): Long = {
    ballot + inc.toLong * ballotOne
  }

  private def incrementBallot(ballot: Long): Long = {
    ballot + ballotOne
  }

  private def checkLeader() {
    val (topProcess:NetAddress, topBallot:Long) = (ballots + (self -> ballot)).maxBy(_._2)
    val top = (topBallot, topProcess)

    if (topBallot < highestBallot){
      while (ballot <= highestBallot){
        ballot = incrementBallotBy(ballot,1);
      }
      leader = None;
    } else {
      if (leader.isEmpty || (topBallot, topProcess) != leader.get){
        highestBallot = topBallot;
        leader = Some(top);
        trigger(BLE_Leader(topProcess, topBallot, configN) -> ble);
      }
    }
  }

  ctrl uponEvent {
    case _: Start =>  {
      startTimer(6000);
    }
  }

  timer uponEvent {
    case CheckTimeout(_) => {
      if (ballots.size + 1 >= (topology.size + 1 / 2)) {
        checkLeader();
      }
        ballots.clear();
        round = round + 1;
        for (p <- topology) {
          if (p != self) {
            trigger(NetMessage(self, p, HeartbeatReq(round, highestBallot)) -> net);
          }
        }
        startTimer(period);
      }
    }


    net uponEvent {
      case NetMessage(src, HeartbeatReq(r, hb)) => {
        if (hb > highestBallot) {
          highestBallot = hb;
        }
        trigger(NetMessage(self, src.src, HeartbeatResp(r, ballot)) -> net);

      }
      case NetMessage(src, HeartbeatResp(r, b)) => {
        if (r == round) {
          ballots += (src.src -> b);

        } else {
          period = period + delta;
        }
      }
    }

}
