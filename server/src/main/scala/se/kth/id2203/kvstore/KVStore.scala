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
package se.kth.id2203.kvstore;

import se.kth.id2203.consensus.{RSM_Command, SC_Decide, SC_Propose, SequenceConsensus}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.network.Network
import se.sics.kompics.sl._

import scala.collection.mutable;

trait ProposedOpTrait extends RSM_Command {
  def source: NetAddress
  def command: Operation
}

case class OperationToPropose(source: NetAddress, command: Operation) extends ProposedOpTrait {
  override def isRead: Boolean = command.opType.equalsIgnoreCase("GET")
}

class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  val consensus = requires[SequenceConsensus];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  private val storage = mutable.Map.empty[String, String]

  //******* Handlers ******
  net uponEvent {
    case NetMessage(header, op: Op) => {
        log.info("Got operation {}!", op);
      }
  }

  //******* Handlers ******
  net uponEvent {
    case NetMessage(src, op: Operation) => {
      op.opType match {
        case "GET" => log.info("Received operation GET from: " + src);
        case "PUT" => log.info("Received operation PUT from: " + src);
        case "CAS" => log.info("Received operation CAS from: " + src);
      }
    }
      val opPropose = OperationToPropose(src.src, op)
      trigger(SC_Propose(opPropose) -> consensus)
      log.info("Triggering the operation: " + src);
    }



  // The decided upon messages
  consensus uponEvent {

    case SC_Decide(OperationToPropose(source: NetAddress, command: Op)) => {
      command.opType match {
        case "GET" =>
          log.info(s"Handling operation {}!", command)

          trigger(NetMessage(self, source, command.response(OpCode.Ok, storage.getOrElse(command.key, "None"))) -> net)
        case "PUT" =>
          log.info(s"Handling operation {}!", command)
          storage += (command.key -> command.value)
          trigger(NetMessage(self, source, command.response(OpCode.Ok, command.value)) -> net)
        case "CAS" =>
          log.info(s"Handling operation {}!", command)
          val result = storage.get(command.key) match {
            case Some(value) => {
              // Only perform the operation if it is the same
              if (command.expected != "" && command.expected == value) {
                storage(command.key) = command.value
              }

              value
            }
            case None => {
              // Only add if it is expected to be empty
              if (command.expected.isEmpty) {
                storage += (command.key -> command.value)
              }

              None
            }
          }
          trigger(NetMessage(self, source, command.response(OpCode.Ok, result.toString)) -> net)
      }
    }
  }
}
// todo: implement PUT, GET, CAS here
