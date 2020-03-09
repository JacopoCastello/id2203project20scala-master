package LookUpTest
import org.scalatest.FlatSpec
import se.kth.id2203.networking.NetAddressConverter
import se.kth.id2203.overlay.LookupTable;

class LookUpTest extends FlatSpec {

  var rDegree = 3; 
  

 
  var adr1 = NetAddressConverter.convert("127.0.0.1:12345");
  var adr2 = NetAddressConverter.convert("127.0.0.1:56789");
  var adr3 = NetAddressConverter.convert("127.0.0.1:56787");
  var adr4 = NetAddressConverter.convert("127.0.0.1:56777");
  var adr5 = NetAddressConverter.convert("127.0.0.1:56757");
  var adr6 = NetAddressConverter.convert("127.0.0.1:56775");
  var adr7 = NetAddressConverter.convert("127.0.0.1:56555");
  var adr8 = NetAddressConverter.convert("127.0.0.1:56550");

  var nodeset = Set(adr1, adr2, adr3, adr4, adr5, adr6, adr7);

  var lut = new LookupTable();
  lut = LookupTable.generate(nodeset, rDegree);
  

  "A LookUpTable" should "be creatable and include all nodes" in {


    
    assert(lut.getNodes().size == nodeset.size)
  }



  it should "return the leader for a valid partition" in {
    var testkey = "I am a key";
    print(lut)
    var leader = lut.lookup(testkey);
    //print("Key is in partition: " + partitionForKey + " \n")
    assert(leader != None)
  }

  it should "manage to lookup its group" in {
    var partitionForNode = lut.getNodesforGroup(adr1);
    //print("Node1 is in partition: " + partitionForNode + " \n")
    assert(!partitionForNode.isEmpty)
  }

  it should "add a node to a group" in {
    lut.addNodetoGroup(adr8,0)
    //println(lut)
    assert(lut.partitions.get(0).get.contains(adr8))
  }

  it should "remove a node from a group" in {
    lut.removeNodefromGroup(adr8,0)
    //println(lut)
    assert(!lut.partitions.get(0).get.contains(adr8))
  }

}
