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
package se.kth.id2203.failuredetector

import se.kth.id2203.networking.{NetAddress, NetMessage}
import se.sics.kompics.timer.{ScheduleTimeout, Timeout}
import se.sics.kompics.{KompicsEvent, ComponentDefinition => _, Port => _}
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl.{ComponentDefinition, Init, PositivePort}
import se.sics.kompics.timer.Timer


case class CheckTimeout(timeout: ScheduleTimeout) extends Timeout(timeout);

case class HeartbeatReply(seq: Int, config: Int) extends KompicsEvent;
case class HeartbeatRequest(seq: Int, config: Int) extends KompicsEvent;



class EPFD(epfdInit: Init[EPFD]) extends ComponentDefinition {


  val timer = requires[Timer];
  val net: PositivePort[Network] = requires[Network]
  val epfd = provides[EventuallyPerfectFailureDetector];

  

  
  val (self, topology, c) = epfdInit match {
    case Init(self: NetAddress, topology: Set[NetAddress], c: Int) => (self, topology, c)
  }

  val delta = 100000

 
  var period = delta
  var alive = topology
  var suspected = Set[NetAddress]();
  var seqnum = 0;

  def startTimer(delay: Long): Unit = {
    val scheduledTimeout  = new ScheduleTimeout(period);
    scheduledTimeout.setTimeoutEvent(CheckTimeout(scheduledTimeout));
    trigger(scheduledTimeout -> timer);
  }

  
  ctrl uponEvent {
    case _: Start =>  {
      startTimer(0)
    }
  }

  timer uponEvent {
    case CheckTimeout(_) =>  {
      if (!alive.intersect(suspected).isEmpty) {

        period = period + delta;

      }

      seqnum = seqnum + 1;

      for (p <- topology) {
        if (!alive.contains(p) && !suspected.contains(p)) {

          suspected = suspected + p;
          log.info("Suspecting " + p)
          
          trigger(NetMessage(self, self, Suspect(p)) -> net);
          suicide()
        } else if (alive.contains(p) && suspected.contains(p)) {
          suspected = suspected - p;
          log.info("Restored: " + p)
          trigger(NetMessage(self, self,Restore(p)) -> net);
        }
        trigger(NetMessage(self, p, HeartbeatRequest(seqnum, c)) -> net);
      }
      alive = Set[NetAddress]();
      startTimer(period);
    }
  }

  net uponEvent {

    case NetMessage(src, HeartbeatRequest(seq, config)) =>  {
      log.info("Being sent from configuration: " + config + " to configuration " + c)
 
        trigger(NetMessage(src, HeartbeatReply(seq, config)) -> net)
     
    }
    case NetMessage(src, HeartbeatReply(seq, config)) => {
      log.info("Being sent from configuration: " + config + " to configuration " + c)
    
        if ((seq == seqnum) || suspected.contains(src.src)) {
          alive = alive + src.src
        }
    
    }
  }
};
