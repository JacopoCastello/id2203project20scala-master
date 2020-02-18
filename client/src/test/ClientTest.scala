import org.scalatest.FlatSpec
import se.kth.id2203.networking.NetAddress
import se.kth.id2203.networking.NetAddressConverter
import se.kth.id2203.overlay.LookupTable
import org.scalatest._
import se.kth.id2203.ParentComponent
import se.kth.id2203.kvstore.{ClientService, OpCode, OpResponse}
import se.kth.id2203.networking._
import se.sics.kompics.network.Address
import se.sics.kompics.simulator.result.SimulationResultSingleton
import se.sics.kompics.simulator.run.LauncherComp
import se.sics.kompics.simulator.{SimulationScenario => JSimulationScenario}
import se.sics.kompics.sl._
import se.sics.kompics.sl.simulator._
import se.kth.id2203.kvstore

import scala.concurrent.Await
import scala.concurrent.duration._

class ClientTest extends FlatSpec {


  //  WORK
  var clientService = new ClientService();

  //  TESTS

  "A GET operation" should "always return an OKResponse code and a value even if it's None" in {

    var input = "op get 45"
    var inputArray: Array[String] = key.split(" ")
    var operationType = inputArray(1).toUpperCase()
    var key = inputArray(2)

    clientService.op(new Op(operationType, key, "", ""))
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status + " Result was: " + r.value);
      assert(r.status == OpCode.Ok && r.value == "None")
    } catch {
      case e: Throwable => logger.error("Error during op.", e);
    }
  }

  "A PUT operation" should "always return an OKResponse code and the value assigned to the key indicated" in {

    var input = "op put 50 800 "
    var inputArray: Array[String] = key.split(" ")
    var operationType = inputArray(1).toUpperCase()
    var key = inputArray(2)
    var value = inputArray(3)

    clientService.op(new Op(operationType, key, value, ""))
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status + " Result was: " + r.value);
      assert(r.status == OpCode.Ok && r.value == value)
    } catch {
      case e: Throwable => logger.error("Error during op.", e);
    }
  }

  "A CAS operation" should "always return an OKResponse code and the old value if the expected value" +
    "matches the current value assigned to the key indicated" in {

    var input = "op put 60 600"
    var inputArray: Array[String] = key.split(" ")
    var operationType = inputArray(1).toUpperCase()
    var key = inputArray(2)
    var value = inputArray(3)
    clientService.op(new Op(operationType, key, value, ""))

    input = "op cas 60 800 600"
    inputArray: Array[String] = key.split(" ")
    operationType = inputArray(1).toUpperCase()
    key = inputArray(2)
    value = inputArray(3)
    var expectedValue = inputArray(4)

    clientService.op(new Op(operationType, key, value, expectedValue))
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status + " Result was: " + r.value);
      assert(r.status == OpCode.Ok && r.value == expectedValue)
    } catch {
      case e: Throwable => logger.error("Error during op.", e);
    }
  }

  "A WRONG CAS operation" should "return an OKResponse code and the value we want to write" in {

    var input = "op put 60 600"
    var inputArray: Array[String] = key.split(" ")
    var operationType = inputArray(1).toUpperCase()
    var key = inputArray(2)
    var value = inputArray(3)
    clientService.op(new Op(operationType, key, value, ""))

    input = "op cas 60 800 666"
    inputArray: Array[String] = key.split(" ")
    operationType = inputArray(1).toUpperCase()
    key = inputArray(2)
    value = inputArray(3)
    var expectedValue = inputArray(4)

    clientService.op(new Op(operationType, key, value, expectedValue))
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation complete! Response was: " + r.status + " Result was: " + r.value);
      assert(r.status == OpCode.Ok && r.value == value)
    } catch {
      case e: Throwable => logger.error("Error during op.", e);
    }
  }


}