import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import utils.{CustomKafkaClient, CustomKafkaServer}

object StreamingDemo {
  def main(args: Array[String]): Unit = {
    val topic = "foo"

    val kafkaServer = new CustomKafkaServer()
    kafkaServer.start()
    kafkaServer.createTopic(topic, 4)

    val conf = new SparkConf().setAppName("StreamingDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))
    val max = 1000

    val props: Properties = CustomKafkaClient.getBasicStringStringConsumer(kafkaServer)

    val kafkaStream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        util.Arrays.asList(topic),
        props.asInstanceOf[java.util.Map[String, Object]]
      )
    )

    kafkaStream.foreachRDD(r => {
      println("*** got an RDD, size = " + r.count())
      r.foreach(s => println(s))
      if (r.count() > 0) {
        println("*** " + r.getNumPartitions + " partitions")
        r.glom().foreach(a => println("*** partition size = " + a.size))
      }
    })
    ssc.start()

    Thread.sleep(5000)
    val producerThread = new Thread("Streaming Termination Controller") {
      override def run(): Unit = {
        val client = new CustomKafkaClient(kafkaServer)

        val numbers = 1 to max

        val producer = new KafkaProducer[String, String](client.basicStringStringProducer)

        numbers.foreach{ n =>
          producer.send(new ProducerRecord(topic, "key_" + n, "string_" + n))
        }
        Thread.sleep(5000)
        println("*** requesting streaming termination")
        ssc.stop(false, true)
      }
    }

    producerThread.start()

    try{
      ssc.awaitTermination()
      println("*** streaming terminated")
    } catch {
      case e: Exception => {
        println("*** streaming exception caught in monitor thread")
      }
    }

    sc.stop()
    kafkaServer.stop()
    println("*** done")
  }
}
