package KafkaLearning

import java.util
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
/**
  * Created by Lalitdutt on 23/2/17.
  **/

/*Custom Partition Implementation */
class KafkaProductSegmentCustomPartition extends Partitioner{
  val productSegmentPartitionService=new ProductSegmentPartitionService()
  override def configure(configs: util.Map[String, _]) {
  }

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster):Int={
    val key_value=key.toString
    val partition_value=productSegmentPartitionService.getID(key_value)
    return partition_value
  }
  override def close() {
  }
}

/*Product Segment Master Model*/
class ProductSegmentPartitionService {
  var segmentData=collection.mutable.Map[String, Int]()

    /*You can replace this part of code with Database connection for Loading Segment Master*/
  {
    segmentData.put("Foods", 0)
    segmentData.put("Electronic and Electrical", 1)
    segmentData.put("Education", 2)
    segmentData.put("Sports", 3)
    segmentData.put("Fashion", 4)
  }
  def getID(name:String): Int ={
    return segmentData.get(name).get
  }
  def getAllSegment():collection.mutable.Map[String,Int]={
    return segmentData
  }
}
