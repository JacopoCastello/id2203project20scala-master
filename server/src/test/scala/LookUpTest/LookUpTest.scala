package LookUpTest
import org.scalatest.FlatSpec
import se.kth.id2203.networking.NetAddress;
import se.kth.id2203.networking.NetAddressConverter;
import se.kth.id2203.overlay.LookupTable;

class LookUpTest extends FlatSpec {


  "A LookUpTable" should "be creatable"

  var adr1 = NetAddressConverter.convert("127.0.0.1:12345");
  var adr2 = NetAddressConverter.convert("127.0.0.1:56789");
  var adr3 = NetAddressConverter.convert("127.0.0.1:56787");
  var adr4 = NetAddressConverter.convert("127.0.0.1:56777");
  var adr5 = NetAddressConverter.convert("127.0.0.1:56757");
  var adr6 = NetAddressConverter.convert("127.0.0.1:56775");
  var adr7 = NetAddressConverter.convert("127.0.0.1:56555");

  var nodest = Set(adr1, adr2, adr3, adr4, adr5, adr6, adr7);

  var lut = new LookupTable();
  lut =  LookupTable.generate(nodest, 3);
  print("Node assignment to partitions: \n"+lut)

  var testkey = "I am a key";
  var partitionForKey = lut.lookup(testkey);
  print("Key is in partition: \n"+partitionForKey)




}
