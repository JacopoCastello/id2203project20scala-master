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

import se.kth.id2203.kvstore.{Op, OpResponse, Operation}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer;

class ScenarioClient extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val timer = requires[Timer];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address");
  var pending: Option[Operation] = None
  var op_index = 0
  var Ops = List.empty[Operation]
  //******* Handlers ******
  /*ctrl uponEvent {
    case _: Start => {
      val messages = SimulationResult[Int]("messages");
      for (i <- 0 to messages) {
        val op = new Op("PUT", "2", "hello");
        val routeMsg = RouteMsg(op.key, op); // don't know which partition is responsible, so ask the bootstrap server to forward it
        trigger(NetMessage(self, server, routeMsg) -> net);
        pending += (op.id -> op.key);
        logger.info("Sending {}", op);
        SimulationResult += (op.key -> "Sent");
      }
    }
  }

  net uponEvent {
    case NetMessage(header, or @ OpResponse(id, status, value)) => {
      logger.debug(s"Got OpResponse: $or");
      pending.remove(id) match {
        case Some(key) => SimulationResult += (key -> status.toString());
        case None      => logger.warn("ID $id was not pending! Ignoring response.");
      }
    }
  }*/
  ctrl uponEvent {
    case _: Start => {
      val op_type: String = SimulationResult[String]("operations")
      val nMessages = SimulationResult[Int]("nMessages")

      if (op_type == "NOP" ) {
        for (i <- 0 to nMessages) {
          Ops ++= List(new se.kth.id2203.kvstore.Op("GET", s"test$i", ""))
        }
      } else if (op_type == "ReadWrite" ) {
        for (i <- 0 to nMessages) {
          Ops ++= List(new Op("PUT", s"test$i", s"$i"))
          Ops ++= List(new Op("GET", s"test$i", ""))
        }
      } else if (op_type == "CAS" ) {
        for (i <- 0 to nMessages) {
          // Shoud result in a list with the values 0..nValues
          if (i < nMessages / 2)
            Ops ++= List(new Op("PUT", s"test$i", s"$i"))
          else
            Ops ++= List(new Op("PUT", s"test$i", s"1"))
        } // Ops = (1,2,3, ..., nMessages/2, 1, 1, ... )

        /*for (i <- 0 to nMessages) {
          if (i < nMessages / 2)
            Ops ++= List(new CompareAndSwap(s"test$i", "1", Some("1"))) // Should not do anything
          else
            Ops ++= List(new CompareAndSwap(s"test$i", s"$i", Some("1"))) // Should replace the second half with correct values
        }*/
        for (i <- 0 to nMessages) {
          Ops ++= List(new Op("GET",s"test$i", ""))
        }
      }


      sendMessage()
    }
  }

  def sendMessage() = {
    val op = Ops(op_index)
    op_index = op_index + 1
    pending = Some(op)
    val routeMsg = RouteMsg(op.key, op) // don't know which partition is responsible, so ask the bootstrap server to forward it
    trigger(NetMessage(self, server, routeMsg) -> net)

    logger.info("Sending {}", op)
    //    SimulationResult += (op.key -> "Sent")
  }

  net uponEvent {
    case NetMessage(header, opResp@OpResponse(id, status, value)) => {
      logger.debug(s"Got OpResponse: $id")

      if (pending.isDefined && pending.get.id == id) {
        val valueString = value match {
          case Some(v) => s"$v";
          case None => "None";
        }

        SimulationResult += (pending.get.key -> valueString)

        printf(s"\nSETTING: ${pending.get.key -> valueString}\n")

        if (op_index < Ops.size) {
          sendMessage()
        }

      } else {
        printf(s"ID was not pending! Ignoring response message: $opResp\n")
      }
      // Should be here!
      //      if (op_index < Ops.size) {
      //        sendMessage()
      //      }
    }
  }
}
