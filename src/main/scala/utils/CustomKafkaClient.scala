package utils

import java.util
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.codehaus.jackson.map.ser.std.StringSerializer

class CustomKafkaClient(server: CustomKafkaServer) {
  def send(topic: String, pairs: Seq[(String, String)]) : Unit = {
    val producer = new KafkaProducer[String, String](basicStringStringProducer)
    pairs.foreach(pair => {
      producer send(new ProducerRecord(topic, pair._1, pair._2))
    })
    producer.close()
  }

  def consumeAndPrint(topic: String, max: Int): Unit = {
    val consumer = new KafkaConsumer[String, String](basicStringStringConsumer);

    consumer.subscribe(util.Arrays.asList(topic))

    var count=0;

    while(count < max){
      println("**** Polling ")

      val records: ConsumerRecords[String, String] = consumer.poll(100)
      println(s"*** received ${records.count} messages")
      count = count + records.count()

      for(rec <- records.records(topic).iterator()){
        println("*** [ " + rec.partition() + " ]" + rec.key() + ":" + rec.value())
      }
    }

    println("*** got the expected number of messages")
    consumer.close()
  }

  def basicStringStringProducer: Properties = {
    val config: Properties = new Properties()
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    config
  }

  def basicStringStringConsumer: Properties = {
    CustomKafkaClient.getBasicStringStringConsumer(server)
  }
}


object CustomKafkaClient{

  def getBasicStringStringConsumer(server: CustomKafkaServer, group:String = "MyGroup"): Properties = {
    val consumerConfig: Properties = new Properties()
    consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server.getKafkaConnect)
    consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig
  }
}