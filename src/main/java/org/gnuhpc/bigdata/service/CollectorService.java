package org.gnuhpc.bigdata.service;

import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.exception.CollectorException;
import org.gnuhpc.bigdata.model.JMXClient;
import org.gnuhpc.bigdata.model.JMXMetricData;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.util.*;

@Service
@Log4j
@Validated
public class CollectorService {
  public List<JMXMetricData> collectJMXData(String jmxurl) {
    List<JMXMetricData> jmxMetricDataList = new ArrayList<>();
    String[] hostList = jmxurl.split(",");
    for (String host : hostList) {
      JMXClient jmxClient = new JMXClient(host);
      Map<String, Object> metricData = new HashMap<>();
      JMXMetricData jmxMetricData = new JMXMetricData(host, metricData);
      try {
        log.info("Start to collect JMXServiceURL:" + jmxClient.getJmxServiceURL());
        jmxClient.connect();
        MBeanServerConnection mBeanServerConnection = jmxClient.getJmxConnector().getMBeanServerConnection();
        Set<ObjectName> objectNames = mBeanServerConnection.queryNames(null, null);
        for (ObjectName objectName : objectNames) {
          //if(objectName.toString().matches("^java.*")) continue;
          Map<String, String> attributeInfoMap = getAttributeInfoByObjectName(mBeanServerConnection, objectName);
          metricData.put(objectName.toString(), attributeInfoMap);
        }
        jmxMetricData.setCollected(true);
      } catch (Exception e) {
        jmxMetricData.setCollected(false);
        CollectorException ce = new CollectorException(String.format("%s occurred. URL: %s. Reason: %s",
                e.getClass().getCanonicalName(), jmxClient.getJmxServiceURL(), e.getCause()), e);
        jmxMetricData.setMsg(ce.getLocalizedMessage());
        log.error("Failed to connect to " + jmxClient.getJmxServiceURL(), ce);
      } finally {
        if (jmxClient.getJmxConnector() != null) {
          try {
            jmxClient.close();
          } catch (Throwable t) {
            log.error("Connection close error occurred. ", t);
          }
        }
        jmxMetricDataList.add(jmxMetricData);
      }
    }

    return jmxMetricDataList;
  }

  private Map<String, String> getAttributeInfoByObjectName(MBeanServerConnection mBeanServerConnection,
                                                   ObjectName objectName) {
    Map<String, String> attributeInfoMap = new HashMap<>();
    try {
      MBeanInfo mbeanInfo = mBeanServerConnection.getMBeanInfo(objectName);
      MBeanAttributeInfo[] mBeanAttributeInfoList = mbeanInfo.getAttributes();
      log.debug("objectName:" + objectName.toString());
      for (MBeanAttributeInfo info : mBeanAttributeInfoList) {
        String attributeName = info.getName();
        String attributeValue = "";
        try {
          attributeValue = mBeanServerConnection.getAttribute(objectName, info.getName()).toString();
        } catch (Exception e) {
          attributeValue = "Unavailable";
          log.info("Exception occured when collect ObjectName:" + objectName + ", AttributeName:" + attributeName, e);
        }
        attributeInfoMap.put(attributeName, attributeValue);
      }
    } catch (Exception e) {
      attributeInfoMap.put("collected", "false");
      log.info("Exception occured when collect ObjectName:" + objectName, e);
    }
    return attributeInfoMap;
  }
}
