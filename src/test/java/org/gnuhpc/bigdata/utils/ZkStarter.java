package org.gnuhpc.bigdata.utils;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

@Log4j2
public class ZkStarter {

//  private static final Logger LOGGER = LoggerFactory.getLogger(ZkStarter.class);
  public static final int DEFAULT_ZK_TEST_PORT = getAvailablePort();
  public static final String DEFAULT_ZK_STR = "localhost:" + DEFAULT_ZK_TEST_PORT;

  private static PublicZooKeeperServerMain _zookeeperServerMain = null;
  private static String _zkDataDir = null;

  private static int getAvailablePort() {
    int port = 0;

    while (true) {
      try {
        port = new Random().nextInt(10000) + 10000;
        (new Socket("127.0.0.1", port)).close();
        new ServerSocket(port).close();
        // Successful connection means the port is taken.
      } catch (Exception e) {
        // Could not connect.
        break;
      }
    }

    return port;
  }

  /**
   * Silly class to make protected methods public.
   */
  static class PublicZooKeeperServerMain extends ZooKeeperServerMain {

    @Override
    public void initializeAndRun(String[] args)
        throws QuorumPeerConfig.ConfigException, IOException {
      super.initializeAndRun(args);
    }

    @Override
    public void shutdown() {
      super.shutdown();
    }
  }

  /**
   * Starts an empty local Zk instance on the default port
   */
  public static void startLocalZkServer() {
    // DEFAULT_ZK_TEST_PORT = new Random().nextInt(10000) + 10000;
    // DEFAULT_ZK_STR = "localhost:" + DEFAULT_ZK_TEST_PORT;
    try {
      startLocalZkServer(DEFAULT_ZK_TEST_PORT);
    } catch (Exception e) {
      log.error("Failed to start ZK: " + e);
    }
  }

  /**
   * Starts a local Zk instance with a generated empty data directory
   *
   * @param port The port to listen on
   */
  public static void startLocalZkServer(final int port) {
    startLocalZkServer(port, org.apache.commons.io.FileUtils.getTempDirectoryPath() + File.separator
        + "test-" + System.currentTimeMillis());
  }

  /**
   * Starts a local Zk instance
   *
   * @param port The port to listen on
   * @param dataDirPath The path for the Zk data directory
   */
  public synchronized static void startLocalZkServer(final int port, final String dataDirPath) {
    if (_zookeeperServerMain != null) {
      throw new RuntimeException("Zookeeper server is already started!");
    }

    // Start the local ZK server
    try {
      _zookeeperServerMain = new PublicZooKeeperServerMain();
      log.info("Zookeeper data path - " + dataDirPath);
      _zkDataDir = dataDirPath;
      final String[] args = new String[]{
          Integer.toString(port), dataDirPath
      };
      new Thread() {
        @Override
        public void run() {
          try {
            _zookeeperServerMain.initializeAndRun(args);
          } catch (QuorumPeerConfig.ConfigException e) {
            log.warn("Caught exception while starting ZK", e);
          } catch (IOException e) {
            log.warn("Caught exception while starting ZK", e);
          }
        }
      }.start();
    } catch (Exception e) {
      log.warn("Caught exception while starting ZK", e);
      throw new RuntimeException(e);
    }

    // Wait until the ZK server is started
    ZkClient client = new ZkClient("localhost:" + port, 10000);
    client.waitUntilConnected(10L, TimeUnit.SECONDS);
    client.close();
  }

  /**
   * Stops a local Zk instance, deleting its data directory
   */
  public static void stopLocalZkServer() {
    try {
      stopLocalZkServer(true);
    } catch (Exception e) {
      log.error("Failed to stop ZK: " + e);
    }
  }

  /**
   * Stops a local Zk instance.
   *
   * @param deleteDataDir Whether or not to delete the data directory
   */
  public synchronized static void stopLocalZkServer(final boolean deleteDataDir) {
    if (_zookeeperServerMain != null) {
      try {
        // Shut down ZK
        _zookeeperServerMain.shutdown();
        _zookeeperServerMain = null;

        // Delete the data dir
        if (deleteDataDir) {
          org.apache.commons.io.FileUtils.deleteDirectory(new File(_zkDataDir));
        }
      } catch (Exception e) {
        log.warn("Caught exception while stopping ZK server", e);
        throw new RuntimeException(e);
      }
    }
  }
}

