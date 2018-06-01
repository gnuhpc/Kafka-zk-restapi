package org.gnuhpc.bigdata.config;

import org.gnuhpc.bigdata.utils.CommonUtils;

import java.io.File;

public class JMXConfig {
  public static final String JMX_WAIT_TIMEOUT = "jmx.wait_timeout";
  public static final String JMX_FETCH_TIMEOUT = "jmx.fetch_timeout";
  public static final String RMI_CONNECT_TIMEOUT = "rmi.connect_timeout";
  public static final String RMI_HANDSHAKE_TIMEOUT = "rmi.handshake_timeout";
  public static final String RMI_RESPONSE_TIMEOUT = "rmi.response_timeout";
  public static final String JMX_PROTOCOL = "service:jmx:rmi:///jndi/rmi://";
  public static final String JMX_FILTER_DIR = CommonUtils.PROJECT_ROOT_FOLDER + File.separator + "JMXFilterTemplate";
}
