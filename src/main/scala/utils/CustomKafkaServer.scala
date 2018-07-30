package utils

import java.io.IOException
import scala.collection.JavaConversions._

import kafka.server.{KafkaConfig, KafkaServerStartable}
import com.typesafe.scalalogging.Logger
import kafka.admin.TopicCommand
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils

@throws[IOException]
class CustomKafkaServer {

  private val LOGGER = Logger[CustomKafkaServer]
  val tempDirs = new TempDirs
  val zkPort = 39001
  val kbPort = 39002
  val zkSessionTimeout = 20000
  val zkConnectionTimeout = 20000

  private var zookeeperHandle: Option[CustomZookeeper] = None
  private var kafkaBrokerHandle: Option[KafkaServerStartable] = None

  def start(): Unit ={
    LOGGER.info(s"Starting on [$zkPort $kbPort]")
    zookeeperHandle = Some(new CustomZookeeper(zkPort, tempDirs))
    zookeeperHandle.get.start

    val kafkaProps = Map(
      "port" -> Integer.toString(kbPort),
      "broker.id" -> "1",
      "host.name" -> "localhost",
      "log.dir" -> tempDirs.kafkaLogDirPath,
      "zookeeper.connect" -> ("localhost:" + zkPort)
    )

    kafkaBrokerHandle = Some(new KafkaServerStartable(serverConfig = new KafkaConfig(kafkaProps)))
    kafkaBrokerHandle.get.startup()
  }

  def stop(): Unit ={
    LOGGER.info(s"Shutting down broker on $kbPort")
    kafkaBrokerHandle match {
      case Some(b) => {
        b.shutdown()
        b.awaitShutdown()
        kafkaBrokerHandle = None
      }
      case None =>
    }

    Thread.sleep(5000)
    LOGGER.info(s"Shutting down zookeeper on $zkPort")
    zookeeperHandle match {
      case Some(zk) => {
        zk.stop()
        zookeeperHandle = None
      }
      case None =>
    }
  }

  def createTopic(topic: String, partitions: Int=1, logAppendTime: Boolean = false): Unit = {
    LOGGER.info(s"Creating [$topic]")

    val arguments = Array[String](
      "--create",
      "--topic",
      topic
    ) ++ (
      if (logAppendTime) {
        Array[String]("--config", "message.timestamp.type=LogAppendTime")
      } else {
        Array[String]()
      }) ++ Array[String](
      "--partitions",
      "" + partitions,
      "--replication-factor",
      "1"
    )

    val opts = new TopicCommand.TopicCommandOptions(arguments)

    val zkUtils = ZkUtils.apply(getZkConnect,
      zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled)

    TopicCommand.createTopic(zkUtils, opts)

    LOGGER.debug(s"Finished creating topic [$topic]")
  }

  def addPartitions(topic: String, partitions: Int): Unit ={
    LOGGER.debug(s"Adding [$partitions] partitions to [$topic]")
    val arguments = Array[String](
      "--alter",
      "--topic",
      topic,
      "--partitions",
      "" + partitions
    )

    val opts = new TopicCommand.TopicCommandOptions(arguments)

    val zkUtils = ZkUtils.apply(getZkConnect,
      zkSessionTimeout, zkConnectionTimeout,
      JaasUtils.isZkSecurityEnabled)

    TopicCommand.alterTopic(zkUtils, opts)
    LOGGER.debug(s"Finished adding [$partitions] partitions to [$topic]")
  }

  def getKafkaConnect: String = "localhost:" + kbPort
  def getZkConnect: String = "localhost:" + zkPort
}
