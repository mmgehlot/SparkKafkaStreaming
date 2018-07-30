package utils

import java.io.IOException
import java.net.InetSocketAddress

import com.typesafe.scalalogging.Logger
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ServerCnxnFactory, ZooKeeperServer}

private[utils] class CustomZookeeper(port: Int, tempDirs: TempDirs) {
  private val LOGGER = Logger[CustomZookeeper]
  private var serverConnectionFactory: Option[ServerCnxnFactory] = None

  def start(): Unit ={
    LOGGER.info(s"Starting Zookeeper on $port")

    try{
      val zkMaxConnections = 32
      val zkTickTime = 2000
      val zkServer = new ZooKeeperServer(tempDirs.zkSnapshotDir, tempDirs.zkLogDir, zkTickTime)
      serverConnectionFactory = Some(new NIOServerCnxnFactory())
      serverConnectionFactory.get.configure(new InetSocketAddress("localhost", port), zkMaxConnections)
      serverConnectionFactory.get.startup(zkServer)
    }
    catch {
      case e: InterruptedException => {
        Thread.currentThread().interrupt()
      }
      case e: IOException => {
        throw new RuntimeException("Unable to start ZooKeeper", e)
      }
    }
  }

  def stop(): Unit ={
    LOGGER.info(s"Shutting down Zookeeper on $port")
    serverConnectionFactory match {
      case Some(f) => {
        f.shutdown()
        serverConnectionFactory = None
      }
      case None =>
    }
  }
}
