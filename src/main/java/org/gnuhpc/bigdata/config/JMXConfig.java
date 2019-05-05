package org.gnuhpc.bigdata.config;

import java.io.File;
import org.gnuhpc.bigdata.utils.CommonUtils;

public class JMXConfig {

  public static final String JMX_CONNECT_TIMEOUT = "attribute.remote.x.request.waiting.timeout";
  public static final String JMX_PROTOCOL = "service:jmx:rmi:///jndi/rmi://";
  public static final String JMX_FILTER_DIR =
      CommonUtils.PROJECT_ROOT_FOLDER + File.separator + "JMXFilterTemplate";
}
