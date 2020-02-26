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

import java.io._
import java.nio.file.{Files, Paths}

import se.kth.id2203.consensus.{RSM_Command, SC_Decide, SC_Propose, SequenceConsensus}
import se.kth.id2203.networking._
import se.kth.id2203.overlay.Routing
import se.sics.kompics.network.Network
import se.sics.kompics.sl._

import scala.collection.mutable
import scala.io.Source;

trait ProposedOpTrait extends RSM_Command {
  def source: NetAddress
  def command: Operation
  /*def confReplicagroup: Set[NetAddress]
  def confNumber: Int
  def confRID: mutable.Map[NetAddress, Int]*/
}

class PersistentStorage(address: String) {
  val dirName = new java.io.File(".").getCanonicalPath + "/server/src/main/scala/se/kth/id2203/kvstore/data/" + address;

  val dir = new File(dirName)
  if (!dir.exists())
    dir.mkdirs()


  def addEntry(key: String, value: String){
    val fileName = dirName + s"/$key"
    val file = new File(fileName)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s"$value")
    bw.close()
  }

  def getValue(key: String): String = {
    val fileName = dirName + s"/$key"
    if (Files.exists(Paths.get(fileName))) {
      Source.fromFile(fileName).mkString
    } else {
      "None"
    }
  }
}

//case class OperationToPropose(source: NetAddress, command: Operation, confReplicagroup: Set[NetAddress], confNumber: Int, confRID: mutable.Map[NetAddress, Int]) extends ProposedOpTrait
case class OperationToPropose(source: NetAddress, command: Operation) extends ProposedOpTrait
class KVService extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network];
  val route = requires(Routing);
  val consensus = requires[SequenceConsensus];
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address");
  private val storage = mutable.Map.empty[String, String]
  log.info(new java.io.File(".").getCanonicalPath)
  val persistentStorage = new PersistentStorage(self.getIp().toString())

  //******* Handlers ******
  net uponEvent {
    case NetMessage(header, op: Op) => {
      val fileName = self.toString +
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
        case "STOP" => log.info("Received operation STOP from: " + src);
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

          //trigger(NetMessage(self, source, command.response(OpCode.Ok, storage.getOrElse(command.key, "None"))) -> net)
          trigger(NetMessage(self, source, command.response(OpCode.Ok, persistentStorage.getValue(command.key))) -> net)
        case "PUT" =>
          log.info(s"Handling operation {}!", command)
          //storage += (command.key -> command.value)
          persistentStorage.addEntry(command.key, command.value)
          log.info("storage at: "+ self + " is "+ storage)
          trigger(NetMessage(self, source, command.response(OpCode.Ok, command.value)) -> net)
        case "CAS" =>
          log.info(s"Handling operation {}!", command)
          val result = persistentStorage.getValue(command.key) match {
          //val result = storage.get(command.key) match {
            case "None" => {
              // Only add if it is expected to be empty
              if (command.expected.isEmpty) {
                //storage += (command.key -> command.value)
                persistentStorage.addEntry(command.key, command.value)
              }
              None
            }
            case value => {
              // Only perform the operation if it is the same
              if (command.expected != "" && command.expected == value) {
                persistentStorage.addEntry(command.key, command.value)
                //storage(command.key) = command.value
              }
              log.info("storage at: "+ self + " is "+ storage)
              value
            }

          }
          trigger(NetMessage(self, source, command.response(OpCode.Ok, result.toString)) -> net)
        case "STOP" =>
           if (source == self){
            log.info(s"KV received Handover from "+source+ "at "+ self)
          }


       
      }
    }
  }
}