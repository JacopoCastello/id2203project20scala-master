package se.kth.id2203.simulation

import java.util.UUID

import se.kth.id2203.kvstore._
import se.kth.id2203.networking._
import se.kth.id2203.overlay.RouteMsg
import se.sics.kompics.Start
import se.sics.kompics.network.Network
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator.SimulationResult
import se.sics.kompics.timer.Timer

import scala.collection.mutable;

class LinTestClient extends ComponentDefinition {

  //******* Ports ******
  val net = requires[Network]
  val timer = requires[Timer]
  //******* Fields ******
  val self = cfg.getValue[NetAddress]("id2203.project.address")
  val server = cfg.getValue[NetAddress]("id2203.project.bootstrap-address")
  private val pending = mutable.Map.empty[UUID, String]

  val trace = mutable.Queue.empty[Op]
  val traceR = mutable.Queue.empty[OpResponse]
  var traceNo = 0;
  var oP : Op = new Op("CAS","","", "")
  var finish = false;
  ctrl uponEvent {
    case _: Start =>  {
      val messages = SimulationResult[Int]("messages")
      for (i <- 0 to messages) {
        val opPUT = new Op("PUT", i.toString, i.toString, " ")
        val routeMsg = RouteMsg(opPUT.key, opPUT) 
        trigger(NetMessage(self, server, routeMsg) -> net)
        trace.enqueue(opPUT)
        pending += (opPUT.id -> opPUT.value)
        logger.info("Sending {}", opPUT)
        

        val opGet = new Op("GET", i.toString, " ", " ")
        val routeMsg1 = RouteMsg(opGet.key, opGet) 
        trigger(NetMessage(self, server, routeMsg1) -> net)
        trace.enqueue(opGet)
        pending += (opGet.id -> opPUT.value)
        logger.info("Sending {}", opGet)
        
      }
      finish = true;

    
    }
  }

  net uponEvent {
 
    case NetMessage(header, or @ OpResponse(id, status, res)) =>  {
      logger.debug(s"Got OpResponse: $or")
      traceR.enqueue(or)
      var correctTrace = true
      SimulationResult += ("finalResult" -> "True")

      if(finish){
    
        var i = 0;
        var lin = true;
        while (i < SimulationResult[Int]("messages")*2 && lin) {
          val opr = trace.dequeue()
          val res = traceR.dequeue()
          if(!opr.id.equals(res.id)){
            SimulationResult += ("finalResult" -> "False")
            lin = false;
          } else{
            if(pending.get(opr.id) == res.value ){
              SimulationResult += ("finalResult" -> "True")
            } else {
              SimulationResult += ("finalResult" -> "False")
              lin = false;
            }

          }
        }
   
      }
    }
  }
}
