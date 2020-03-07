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
package se.kth.id2203.simulation;

import java.util.UUID

import se.kth.id2203.kvstore.{Op, OpResponse, Operation}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import scala.collection.mutable;

class ScenarioClient extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val timer = requires[Timer];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  private val pending = mutable.Map.empty[UUID, String];
  var op_index = 0
  var Ops = List.empty[Operation]
  //******* Handlers ******
  ctrl uponEvent {
    case _: Start => {
      val messages = SimulationResult[Int]("nMessages");
      val op_type: String = SimulationResult[String]("operations")
      if (op_type == "SimpleOperation" ) {
        for (i <- 0 to messages) {
          Ops ++= List(new Op("GET", s"test$i", "", "")) 
        }
      } else if (op_type == "Write" ) {
        for (i <- 0 to messages) {
          Ops ++= List(new Op("PUT", s"test$i", s"$i", "")) 
          Ops ++= List(new Op("GET", s"test$i", "", "")) 

        }
        for (i <- 0 to messages) {
          Ops ++= List(new Op("GET", s"test$i", "", "")) 
        }
      } else if (op_type == "CAS") {
        for (i <- 0 to messages) {
          // Shoud result in a list with the values 0..nValues
          if (i < messages / 2)
            Ops ++= List(new Op("PUT", s"test$i", s"$i", ""))
          else
            Ops ++= List(new Op("PUT", s"test$i", s"1", ""))
        } // Ops = (1,2,3, ..., nMessages/2, 1, 1, ... )

        for (i <- 0 to messages) {
          if (i < messages / 2)
            Ops ++= List(new Op("CAS", s"test$i", "1", "1")) 
          else
            Ops ++= List(new Op("CAS", s"test$i", s"$i", "1")) 
        }
        for (i <- 0 to messages) {
          Ops ++= List(new Op("GET", s"test$i", "", ""))
        }
      }
      send();
    }
  }

  def send() ={
    var op_index = 0;
    while (op_index < Ops.size) {
      val op = Ops(op_index)
      val routeMsg = RouteMsg(op.key, op); 
      trigger(NetMessage(self, server, routeMsg) -> net);
      pending += (op.id -> op.key);
      logger.info("Sending {}", op);
      op_index += 1
    }
  }

  net uponEvent {
    case NetMessage(header, or @ OpResponse(id, status, value)) => {
      logger.debug(s"Got OpResponse: $or");
      pending.remove(id) match {
        case Some(key) => SimulationResult += (key -> value.toString());
        case None      => logger.warn("ID $id was not pending! Ignoring response.");
      }
    }
  }
}
