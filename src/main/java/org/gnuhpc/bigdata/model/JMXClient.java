package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.config.JMXConfig;
import org.gnuhpc.bigdata.exception.CollectorException;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkNotNull;

@Getter
@Setter
@Log4j
public class JMXClient {
  private String ip;
  private String port;
  private JMXConnector jmxConnector = null;
  private static final ThreadFactory daemonThreadFactory = new DaemonThreadFactory();
  private String jmxServiceURL;
  private Map<String, Object> jmxEnv;
  private static final long CONNECTION_TIMEOUT = 10000;
  private static final long JMX_TIMEOUT = 20;

  public JMXClient() {
    jmxEnv = new HashMap<>();
    jmxEnv.put(JMXConfig.JMX_CONNECT_TIMEOUT, CONNECTION_TIMEOUT);
  }

  public JMXClient(String host) {
    this();
    String[] ipAndPort = host.split(":");
    this.ip = ipAndPort[0];
    this.port = ipAndPort[1];
    this.jmxServiceURL = new StringBuilder().append(JMXConfig.JMX_PROTOCOL)
            .append(this.ip)
            .append(":")
            .append(this.port)
            .append("/jmxrmi").toString();
  }

  public JMXConnector connect() throws CollectorException {
    try {
      JMXServiceURL jmxServiceURL = new JMXServiceURL(this.jmxServiceURL);
      jmxConnector = JMXConnectorFactory.connect(jmxServiceURL, jmxEnv);
    } catch (MalformedURLException e) {
      throw new CollectorException(String.format("%s occurred. URL: %s. Reason: %s",
              e.getClass().getCanonicalName(), this.jmxServiceURL, e.getCause()), e);
    } catch (IOException e) {
      throw new CollectorException(String.format("%s occurred. URL: %s. Reason: %s",
              e.getClass().getCanonicalName(), this.jmxServiceURL, e.getCause()), e);
    }
    return jmxConnector;
  }

  /**
   * This code comes from Datadog jmxFetch.
   * https://github.com/DataDog/jmxfetch/blob/master/src/main/java/org/datadog/jmxfetch/Connection.java
   */
  public JMXConnector connectWithTimeout() throws IOException, InterruptedException {
    JMXServiceURL url = new JMXServiceURL(this.jmxServiceURL);

    BlockingQueue<Object> mailbox = new ArrayBlockingQueue<Object>(1);

    ExecutorService executor = Executors.newSingleThreadExecutor(daemonThreadFactory);
    executor.submit(() -> {
      try {
        JMXConnector connector = JMXConnectorFactory.connect(url, jmxEnv);
        if (!mailbox.offer(connector)) {
          connector.close();
        }
      } catch (Throwable t) {
        mailbox.offer(t);
      }
    });
    Object result;
    try {
      result = mailbox.poll(JMX_TIMEOUT, TimeUnit.SECONDS);
      if (result == null) {
        if (!mailbox.offer(""))
          result = mailbox.take();
      }
    } catch (InterruptedException e) {
      throw e;
    } finally {
      executor.shutdown();
    }
    if (result == null) {
      log.warn("Connection timed out: " + url);
      throw new SocketTimeoutException("Connection timed out: " + url);
    }
    if (result instanceof JMXConnector) {
      jmxConnector = (JMXConnector) result;
      return jmxConnector;
    }
    try {
      throw (Throwable) result;
    } catch (Throwable e) {
      throw new IOException(e.toString(), e);
    }
  }

  public void close() throws CollectorException {
    checkNotNull(jmxConnector);
    try {
      jmxConnector.close();
    } catch (IOException e) {
      throw new CollectorException("Cannot close connection. ", e);
    }
  }

  private static class DaemonThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setDaemon(true);
      return t;
    }
  }
}
