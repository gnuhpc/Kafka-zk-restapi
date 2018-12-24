package org.gnuhpc.bigdata.service;

import lombok.extern.log4j.Log4j;
import org.gnuhpc.bigdata.config.JMXConfig;
import org.gnuhpc.bigdata.exception.CollectorException;
import org.gnuhpc.bigdata.model.*;
import org.gnuhpc.bigdata.utils.CommonUtils;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Log4j
@Validated
public class CollectorService {
  private final static List<String> SIMPLE_TYPES = Arrays.asList("long",
          "java.lang.String", "int", "float", "double", "java.lang.Double","java.lang.Float", "java.lang.Integer", "java.lang.Long",
          "java.util.concurrent.atomic.AtomicInteger", "java.util.concurrent.atomic.AtomicLong",
          "java.lang.Object", "java.lang.Boolean", "boolean", "java.lang.Number");
  private final static List<String> COMPOSED_TYPES = Arrays.asList("javax.management.openmbean.CompositeData", "java.util.HashMap", "java.util.Map");
  private final static List<String> MULTI_TYPES = Arrays.asList("javax.management.openmbean.TabularData");

  public List<JMXMetricDataV1> collectJMXData(String jmxurl) {
    LinkedList<JMXMetricDataV1> jmxMetricDataList = new LinkedList<>();
    String[] hostList = jmxurl.split(",");
    for (String host : hostList) {
      JMXClient jmxClient = new JMXClient(host);
      Map<String, Object> metricData = new HashMap<>();
      JMXMetricDataV1 jmxMetricData = new JMXMetricDataV1(host, metricData);
      try {
        log.info("Start to collect JMXServiceURL:" + jmxClient.getJmxServiceURL());
        jmxClient.connectWithTimeout();
        MBeanServerConnection mBeanServerConnection = jmxClient.getJmxConnector().getMBeanServerConnection();
        Set<ObjectName> objectNames = mBeanServerConnection.queryNames(null, null);
        for (ObjectName objectName : objectNames) {
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
        jmxMetricDataList.add(jmxMetricData);
        if (jmxClient.getJmxConnector() != null) {
          try {
            jmxClient.close();
          } catch (Throwable t) {
            log.error("Connection close error occurred. ", t);
          }
        }
      }
    }

    return jmxMetricDataList;
  }

  public List<JMXMetricData> collectJMXData(String jmxurl, JMXQuery jmxQuery) {
    List<JMXMetricData> jmxMetricDataList = new ArrayList<>();
    LinkedList<JMXConfiguration> configurationList = jmxQuery.getFilters();
    LinkedList<String> beanScopes = JMXConfiguration.getGreatestCommonScopes(configurationList);
    Set<ObjectName> beans = new HashSet<>();
    LinkedList<JMXAttribute> matchingAttributes = new LinkedList<>();
    LinkedList<HashMap<String, Object>> metrics = new LinkedList<>();

    String[] hostList = jmxurl.split(",");

    for (String host : hostList) {
      JMXClient jmxClient = new JMXClient(host);
      beans.clear();
      matchingAttributes.clear();
      metrics.clear();
      JMXMetricData jmxMetricData = new JMXMetricData(host, metrics);
      try {
        jmxClient.connectWithTimeout();
        MBeanServerConnection mBeanServerConnection = jmxClient.getJmxConnector().getMBeanServerConnection();
        for (String scope : beanScopes) {
          ObjectName name = new ObjectName(scope);
          beans.addAll(mBeanServerConnection.queryNames(name, null));
        }
        beans = (beans.isEmpty()) ? mBeanServerConnection.queryNames(null, null) : beans;
        getMatchingAttributes(matchingAttributes, mBeanServerConnection, beans, configurationList);
        jmxMetricData.setMetrics(getMetrics(matchingAttributes));
        jmxMetricData.setCollected(true);
      } catch (Exception e) {
        jmxMetricData.setCollected(false);
        CollectorException ce = new CollectorException(String.format("%s occurred. URL: %s. Reason: %s",
                e.getClass().getCanonicalName(), jmxClient.getJmxServiceURL(), e.getCause()), e);
        jmxMetricData.setMsg(ce.getLocalizedMessage());
        log.error("Failed to connect to " + jmxClient.getJmxServiceURL(), ce);
      } finally {
        jmxMetricDataList.add(jmxMetricData);
        try {
          if (jmxClient.getJmxConnector() != null) {
            jmxClient.close();
          }
        } catch (Throwable t) {
          log.error("Connection close error occurred. ", t);
        }
      }
    }
    return jmxMetricDataList;
  }

  private void getMatchingAttributes(LinkedList<JMXAttribute> matchingAttributes, MBeanServerConnection mBeanServerConnection, Set<ObjectName> beans,
                                     LinkedList<JMXConfiguration> configurationList) {
    for (ObjectName beanName : beans) {
      MBeanAttributeInfo[] attributeInfos;
      try {
        attributeInfos = mBeanServerConnection.getMBeanInfo(beanName).getAttributes();
      } catch (Exception e) {
        CollectorException ce = new CollectorException(String.format("Get bean's attributes exception. BeanName: %s. Reason: %s",
                beanName, e.getCause()), e);
        log.error("Failed to get bean attributes. BeanName is " + beanName, ce);
        continue;
      }

      for (MBeanAttributeInfo attributeInfo: attributeInfos) {
        JMXAttribute jmxAttribute;
        String attributeType = attributeInfo.getType();
        if (SIMPLE_TYPES.contains(attributeType)) {
          log.debug(beanName + " : " + attributeInfo + " has attributeInfo simple type");
          jmxAttribute = new JMXSimpleAttribute(attributeInfo, beanName, mBeanServerConnection);
        } else if (COMPOSED_TYPES.contains(attributeType)) {
          log.debug(beanName + " : " + attributeInfo + " has attributeInfo composite type");
          jmxAttribute = new JMXComplexAttribute(attributeInfo, beanName, mBeanServerConnection);
        } else if (MULTI_TYPES.contains(attributeType)) {
          log.debug(beanName + " : " + attributeInfo + " has attributeInfo tabular type");
          jmxAttribute = new JMXTabularAttribute(attributeInfo, beanName, mBeanServerConnection);
        } else {
          //try {
            log.debug(beanName + " : " + attributeInfo + " has an unsupported type: " + attributeType);
          //} catch (NullPointerException e) {
          //  log.error("Caught unexpected NullPointerException");
          //}
          continue;
        }
        for (JMXConfiguration conf: configurationList) {
          if (jmxAttribute.match(conf)) {
            jmxAttribute.setMatchingConf(conf);
            matchingAttributes.add(jmxAttribute);
            log.debug("       Matching Attribute: " + jmxAttribute.getAttributeName() +
                      ", BeanName:" + beanName.getCanonicalName());
          }
        }
      }
    }
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

  public LinkedList<HashMap<String, Object>> getMetrics(LinkedList<JMXAttribute> matchingAttributes) throws IOException {
    LinkedList<HashMap<String, Object>> metrics = new LinkedList<HashMap<String, Object>>();
    Iterator<JMXAttribute> it = matchingAttributes.iterator();

    while (it.hasNext()) {
      JMXAttribute jmxAttr = it.next();
      try {
        LinkedList<HashMap<String, Object>> jmxAttrMetrics = jmxAttr.getMetrics();
        for (HashMap<String, Object> m : jmxAttrMetrics) {
          //m.put("check_name", this.checkName);
          metrics.add(m);
          JSONObject metricJson = new JSONObject(m);
        }
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        log.debug("Cannot get metrics for attribute: " + jmxAttr, e);
      }
    }

    return metrics;
  }

  public HashMap<String, Object> listJMXFilterTemplate(String filterKey) {
    HashMap<String, Object> filterTemplateMap = new HashMap<>();
    HashMap<Object, Object> yamlHash;
    String projectRootPath = "";
    try {
      File jmxFilterDir = new File(JMXConfig.JMX_FILTER_DIR);
      if (!jmxFilterDir.exists() || !jmxFilterDir.isDirectory()) {
        throw new IOException();
      }
      for (File yamlFile:jmxFilterDir.listFiles()) {
        String fileFullName = yamlFile.getName();
        log.info("Found JMXFilterTemplate filename=" + fileFullName);
        if (matchIgnoreCase(filterKey, fileFullName)) {
          String[] fileNames = fileFullName.split("\\.");
          yamlHash = CommonUtils.yamlParse(yamlFile);
          filterTemplateMap.put(fileNames[0], yamlHash);
        }
      }
    } catch (IOException e) {
      CollectorException ce = new CollectorException(String.format("%s occurred. Reason:%s. Advice:"+
                      "Create a directory named JMXFilterTemplate to include filter templates in the project root path:%s.",
              e.getClass().getCanonicalName(), e.getLocalizedMessage(), projectRootPath), e);
      log.error("JMXFilterTemplate path does not exist.");
      filterTemplateMap.put("error", ce.getLocalizedMessage());
    }

    return filterTemplateMap;
  }

  boolean matchIgnoreCase(String regex, String string) {
    Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(string);

    boolean match = matcher.find();

    return match;
  }
}
