package KafkaLearning
import java.util.Properties
import org.apache.kafka.clients.producer._
/**
  * Created by LALITDUTT on 22/2/17.
  */

object CustomPartitioner{
  /*To Execute Program*/
  def main(args: Array[String]): Unit = {
    //Single Broker
    val brokers = "localhost:9092"
    //Kafka Topic With 5 partition
    val topic = "productdata"
    val productDataProducer=new ProductDataProducer()
    val producerProp=productDataProducer.getProducerProperties(brokers)
    productDataProducer.sendMessage(topic,producerProp)
  }


  /* Product Data Producer with custom partitioner {[Number Of Partition:5 (0 to 4)],[Partition Based On:Product Segment]}  */
  class ProductDataProducer{
    def getProducerProperties(brokers:String):Properties={
      var props = new Properties()
      props.put("bootstrap.servers", brokers)
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("partitioner.class","KafkaLearning.KafkaProductSegmentCustomPartition")
      return props
    }

    def sendMessage(topic:String,props:Properties):Unit={
      val productPartitionService=new ProductSegmentPartitionService()
      val producerUser=new KafkaProducer[String,String](props)
      for(product<-productPartitionService.getAllSegment()){
        val msg="""{"data":{"product_name":"dummy product title","segment":""""+product._1+""""}}"""
        println(msg)
        val record = new ProducerRecord[String, String](topic,product._1, msg)
        producerUser.send(record)
      }
      producerUser.close()
    }
  }
}



