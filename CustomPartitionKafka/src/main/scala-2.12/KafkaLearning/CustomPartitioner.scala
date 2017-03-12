package KafkaLearning

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._

/**
  * Created by LALITDUTT on 22/2/17.
  */

object CustomPartitioner{

  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
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
      for(product<-productPartitionService.getAllUser()){
        val msg="Product "+product._1
        val record = new ProducerRecord[String, String](topic,product._1, msg)
        producerUser.send(record)
      }
      producerUser.close()
    }
  }


}



