package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import org.gnuhpc.bigdata.config.JMXConfig;
import org.gnuhpc.bigdata.exception.CollectorException;

import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Getter
@Setter
public class JMXClient {
  private String ip;
  private String port;
  private JMXConnector jmxConnector;
  private String jmxServiceURL;
  private Map<String, Object> jmxEnv;

  public JMXClient() {
    jmxEnv = new HashMap<>();
    jmxEnv.put(JMXConfig.JMX_WAIT_TIMEOUT, 3000);
    jmxEnv.put(JMXConfig.JMX_FETCH_TIMEOUT, 3000);
    jmxEnv.put(JMXConfig.RMI_CONNECT_TIMEOUT, 3000);
    jmxEnv.put(JMXConfig.RMI_HANDSHAKE_TIMEOUT, 3000);
    jmxEnv.put(JMXConfig.RMI_RESPONSE_TIMEOUT, 3000);
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

  public void close() throws CollectorException {
    checkNotNull(jmxConnector);
    try {
      jmxConnector.close();
    } catch (IOException e) {
      throw new CollectorException(String.format("Cannot close connection. "), e);
    }
  }
}
