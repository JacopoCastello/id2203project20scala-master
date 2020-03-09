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
package se.kth.id2203.simulation

import java.io.File
import java.net.{InetAddress, UnknownHostException}
import org.scalatest._
import se.kth.id2203.ParentComponent
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._
import scala.concurrent.duration._
import scala.reflect.io.Directory


class ReconfigurationTest extends FlatSpec with Matchers {

  private val nMessages = 10;


  "Simple Operations" should "return None" in {
    val seed = 123l;
    JSimulationScenario.setSeed(seed);
    val simpleBootScenario = SimpleScenarioReconfiguration.scenario(8);
    val res = SimulationResultSingleton.getInstance();
    SimulationResult += ("operations" -> "SimpleOperation")
    SimulationResult += ("nMessages" -> nMessages);
    simpleBootScenario.simulate(classOf[LauncherComp]);
    
     for (i <- 0 to nMessages) {
       SimulationResult.get[String](s"test$i") should be(Some("None"));

     }
    deletePersistentStorage()
  }

  "Write then Read" should "read the writen value" in {
      def clockTime: Long = {
        System.currentTimeMillis()
      }

      var starttime = clockTime
      println("start time:" + starttime)
      val seed = 123l
      JSimulationScenario.setSeed(seed)
      val simpleBootScenario = SimpleScenarioReconfiguration.scenario(8)
      val res = SimulationResultSingleton.getInstance()

      SimulationResult += ("operations" -> "Write")
      SimulationResult += ("nMessages" -> nMessages)

      simpleBootScenario.simulate(classOf[LauncherComp])

      for (i <- 0 to nMessages) {
        SimulationResult.get[String](s"test$i") should be(Some((s"$i")))
      }
      var endtime = clockTime
      println("end time:" + endtime)
      var timedif = (endtime - starttime)
      println("time difference: " + (endtime - starttime))
      deletePersistentStorage()
  }

  "Compare and swap" should "swap the values if they are correct" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = SimpleScenario.scenario(8)
    val res = SimulationResultSingleton.getInstance()

    SimulationResult += ("operations" -> "CAS")
    SimulationResult += ("nMessages" -> nMessages)

    simpleBootScenario.simulate(classOf[LauncherComp])

    for (i <- 0 to nMessages) {
      if (i < nMessages/2) {
        SimulationResult.get[String](s"test$i") should be(Some((s"$i")))
      } else {
        SimulationResult.get[String](s"test$i") should be(Some("1"))
      }
    }
    deletePersistentStorage()
  }

  "Write then Read Lease" should "read the written value and track the time" in {
    
    var res:Map[Int,Long]=Map()
    val nMessagesL = Seq(10, 100, 1000)
   
    val rounds = 5
    for(msg <-nMessagesL) {
      var sum =0l
      for (i <- 1 to rounds) {
        def clockTime: Long = {
          System.currentTimeMillis()
        }

        var starttime = clockTime
        println("start time:" + starttime)
        val seed = 123l
        JSimulationScenario.setSeed(seed)
        val simpleBootScenario = SimpleScenarioReconfiguration.scenario(8)
        val res = SimulationResultSingleton.getInstance()

        SimulationResult += ("operations" -> "Write")
        SimulationResult += ("nMessages" -> msg)

        simpleBootScenario.simulate(classOf[LauncherComp])

        for (i <- 0 to msg) {
          SimulationResult.get[String](s"test$i") should be(Some((s"$i")))
        }
        var endtime = clockTime
        println("end time:" + endtime)
        var timedif = (endtime - starttime)
        sum += timedif
        println("time difference: " + (endtime - starttime))
        deletePersistentStorage()
      }
      res += (msg -> (sum / rounds) )
      println("Average time for " + msg + " is: " + sum / rounds)
    }
    println(res)
    println("lease test done! " )
  }


  def deletePersistentStorage(): Unit ={
    val path = new java.io.File(".").getCanonicalPath;
    val directory = new Directory(new File((path+"/server/src/main/scala/se/kth/id2203/kvstore/data")))
    directory.deleteRecursively()
  }
}

object SimpleScenarioReconfiguration {

  import Distributions._
  
  implicit val random = JSimulationScenario.getRandom();

  private def intToServerAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.0." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }
  private def intToClientAddress(i: Int): Address = {
    try {
      NetAddress(InetAddress.getByName("192.193.1." + i), 45678);
    } catch {
      case ex: UnknownHostException => throw new RuntimeException(ex);
    }
  }

  private def isBootstrap(self: Int): Boolean = self == 1;

  var killed = false
  private def isKilled(): Boolean = killed == true;
  private def setKilled(): Unit = killed = true;
  

  val startServerOp = Op { (self: Integer) =>

    val selfAddr = intToServerAddress(self)
    val conf = if (isBootstrap(self)) {
     
      Map("id2203.project.address" -> selfAddr)
    } else {
      Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1))
    };
    StartNode(selfAddr, Init.none[ParentComponent], conf);
  };

  val stopServerOp = Op { (self: Integer) =>
    val selfAddr = intToServerAddress(self)
    setKilled()
    KillNode(selfAddr);

  };

  val startClientOp = Op { (self: Integer) =>
    val selfAddr = intToClientAddress(self)
    if (!isKilled()) {
      val conf = Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(1));
      StartNode(selfAddr, Init.none[ScenarioClient], conf);
    } else {
      val conf = Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(2));
      StartNode(selfAddr, Init.none[ScenarioClient], conf);
    }
  };

  def scenario(servers: Int): JSimulationScenario = {

    //val networkSetup = raise(1, setUniformLatencyNetwork()).arrival(constant(0));
    val startCluster = raise(servers, startServerOp, 1.toN).arrival(constant(1.second));
    val startClients = raise(1, startClientOp, 1.toN).arrival(constant(1.second));
    val stopServerOp = raise(1, this.stopServerOp, 1.toN).arrival(constant(1.second));
    startCluster andThen
      100.seconds afterTermination startClients andThen
      10.seconds afterTermination stopServerOp andThen
      10000.seconds afterTermination startClients andThen
      10000.seconds afterTermination Terminate
  }



}
