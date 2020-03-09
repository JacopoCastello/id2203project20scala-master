
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
package se.kth.id2203.overlay;

import com.larskroll.common.collections._
import se.kth.id2203.bootstrapping.NodeAssignment
import se.kth.id2203.networking.NetAddress

import scala.collection.mutable;



@SerialVersionUID(6322485231428233902L)
class LookupTable extends NodeAssignment with Serializable {
  val nodesInPartition = 3;
  val partitions = TreeSetMultiMap.empty[Int, NetAddress]; 
  var leader = mutable.Map.empty[Int, NetAddress];


  def lookup(key: String): NetAddress = {
    val keyHash = math.abs(key.hashCode()); 
    val partitionIdx = keyHash % partitions.keySet.size 

   
    return leader(partitionIdx)
  }

  
  def getNodesforGroup(node: NetAddress): Set[NetAddress] = partitions.filter(partition => partition._2.iterator.contains(node)).foldLeft(Set.empty[NetAddress]) {
    case (acc, kv) => acc ++ kv._2
  }

  def getKeyforNode(node: NetAddress): Int = {
    val entry =  partitions.filter(partition =>  partition._2.iterator.contains(node)).toList
    if (entry.size>0) {
      return entry(0)._1
    }else{ 
      return -1
    }
  }

  def getNodes(): Set[NetAddress] = partitions.foldLeft(Set.empty[NetAddress]) {
    case (acc, kv) => acc ++ kv._2
  }


 
  def addNodetoGroup(node: NetAddress, partitionIdx: Int) {
    partitions.put(partitionIdx -> node);
    true 
  }

  
  def removeNodefromGroup(node: NetAddress, partitionIdx: Int): Boolean ={
    if(partitions.get(partitionIdx).get.contains(node)){
      partitions.remove(partitionIdx -> node);
      true
    }else{
      false
    }
  }

  def setNewLeader(node: NetAddress, partitionIdx: Int): Boolean = {
    leader += (partitionIdx -> node)
    true
  }

  override def toString(): String = {
    val sb = new StringBuilder();
    sb.append("LookupTable(\n");
    sb.append(partitions.mkString(","));
    sb.append(")");
    return sb.toString();
  }
}

object LookupTable {

  
  def generate(nodes: Set[NetAddress], rDegree: Int ): LookupTable = { 
    val lut = new LookupTable();
    var availablePartitions = math.floor(nodes.size / rDegree).toInt; 
    var idxIterator = 0;
    var round = 0;

    for(node <- nodes){ 
      lut.partitions.put(idxIterator -> node);
      if (round == 0){
        lut.leader += (idxIterator -> node)
      }
      idxIterator+= 1;
      if (idxIterator == availablePartitions){
        idxIterator = 0;
        round += 1
      }
    }
    lut
  }

}

