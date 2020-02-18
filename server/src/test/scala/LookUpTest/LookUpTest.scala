package LookUpTest
import org.scalatest.FlatSpec
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.networking.NetAddressConverter;
import se.kth.id2203.overlay.LookupTable;

class LookUpTest extends FlatSpec {

  var rDegree = 3; // 3 nodes as a minimum for each group

  // some mock node addresses
  var adr1 = NetAddressConverter.convert("127.0.0.1:12345");
  var adr2 = NetAddressConverter.convert("127.0.0.1:56789");
  var adr3 = NetAddressConverter.convert("127.0.0.1:56787");
  var adr4 = NetAddressConverter.convert("127.0.0.1:56777");
  var adr5 = NetAddressConverter.convert("127.0.0.1:56757");
  var adr6 = NetAddressConverter.convert("127.0.0.1:56775");
  var adr7 = NetAddressConverter.convert("127.0.0.1:56555");

  var nodeset = Set(adr1, adr2, adr3, adr4, adr5, adr6, adr7);

  var lut = new LookupTable();
  lut = LookupTable.generate(nodeset, rDegree);
  //print("Node assignment to partitions: " + lut + "\n")

  var partitionForNode = lut.getNodesforGroup(adr1);

  "A LookUpTable" should "be creatable and include all nodes" in {


    //print("get nodes : " + lut.getNodes().size + " \n")
    assert(lut.getNodes().size == nodeset.size)
  }

  it should "assign keys to a valid partition" in {
    var testkey = "I am a key";
    var partitionForKey = lut.lookup(testkey);
    //print("Key is in partition: " + partitionForKey + " \n")
    assert(!partitionForKey.isEmpty)
  }

  it should "manage to lookup its group" in {

    //print("Node1 is in partition: " + partitionForNode + " \n")
    assert(!partitionForNode.isEmpty)
  }


  it should "add nodes to groups" in {
    var sizebefore = partitionForNode.size
    var key = lut.getKeyforNode(adr1)
    //print("Node1 is in partition: " + key + " \n")
    var adr8 = NetAddressConverter.convert("127.0.0.1:56888");
    lut = LookupTable.addNodetoGroup(adr8, key, lut)
    assert(lut.getNodesforGroup(adr1).size == sizebefore+1)
  }

}
