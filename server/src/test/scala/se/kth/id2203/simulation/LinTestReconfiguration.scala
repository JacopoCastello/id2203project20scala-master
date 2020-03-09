package se.kth.id2203.simulation

import java.io.File
import java.net.{InetAddress, UnknownHostException}

import org.scalatest._

import scala.concurrent.duration._
import org.scalatest._

import scala.concurrent.duration._
import scala.reflect.io.Directory


class LinTestReconfiguration extends FlatSpec with Matchers {
  private val nMessages = 1000
  private val clusterSize = 6


  "Linearizability Reconfiguration" should "be implemented" in {
    val seed = 123l
    JSimulationScenario.setSeed(seed)
    val simpleBootScenario = LinTestReconfigurationScenario.scenario(clusterSize)
    val res = SimulationResultSingleton.getInstance()
    SimulationResult += ("messages" -> nMessages)

    val messages = SimulationResult[Int]("messages")

    simpleBootScenario.simulate(classOf[LauncherComp])

    

    SimulationResult.get[String]("finalResult") should be (Some("True"))
    deletePersistentStorage()

  }

  def deletePersistentStorage(): Unit ={
    val path = new java.io.File(".").getCanonicalPath;
    val directory = new Directory(new File((path+"/server/src/main/scala/se/kth/id2203/kvstore/data")))
    directory.deleteRecursively()
  }


}



object LinTestReconfigurationScenario {

  import Distributions._
  // needed for the distributions, but needs to be initialised after setting the seed
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
      // don't put this at the bootstrap server, or it will act as a bootstrap client
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
      StartNode(selfAddr, Init.none[LinTestClient], conf);
    } else {
      val conf = Map(
        "id2203.project.address" -> selfAddr,
        "id2203.project.bootstrap-address" -> intToServerAddress(2));
      StartNode(selfAddr, Init.none[LinTestClient], conf);
    }
  };

  def scenario(servers: Int): JSimulationScenario = {

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
