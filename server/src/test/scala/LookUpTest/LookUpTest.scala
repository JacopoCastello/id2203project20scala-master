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

  var nodest = Set(adr1, adr2, adr3);

  var lut = new LookupTable();
  lut =  LookupTable.generate(nodest);
  print(lut)

}
