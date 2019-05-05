package org.gnuhpc.bigdata.model;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import java.io.IOException;
import java.util.*;

public class JMXComplexAttribute extends JMXAttribute {
  private HashMap<String, HashMap<String, Object>> subAttributeList;

  public JMXComplexAttribute(MBeanAttributeInfo attribute, ObjectName beanName, MBeanServerConnection connection) {
    super(attribute, beanName, connection);
    this.subAttributeList = new HashMap<>();
  }

  @Override
  public LinkedList<HashMap<String, Object>> getMetrics()
          throws AttributeNotFoundException, InstanceNotFoundException,
          MBeanException, ReflectionException, IOException {

    LinkedList<HashMap<String, Object>> metrics = new LinkedList<HashMap<String, Object>>();

    for (Map.Entry<String, HashMap<String, Object>> pair : subAttributeList.entrySet()) {
      String subAttribute = pair.getKey();
      HashMap<String, Object> metric = pair.getValue();
      if (metric.get(ALIAS) == null) {
        metric.put(ALIAS, convertMetricName(getAlias(subAttribute)));
      }
      if (metric.get(METRIC_TYPE) == null) {
        metric.put("domain", getBeanName().getDomain());
        metric.put("beanName", getBeanName().toString());
        metric.put("attributeName", subAttribute);
        metric.put(METRIC_TYPE, getMetricType(subAttribute));
      }

      /*
      if (metric.get("tags") == null) {
        metric.put("tags", getTags());
      }
      */

      metric.put("value", castToDouble(getValue(subAttribute), subAttribute));
      metrics.add(metric);

    }
    return metrics;

  }

  private Object getMetricType(String subAttribute) {
    String subAttributeName = getAttribute().getName() + "." + subAttribute;
    String metricType = null;

    JMXFilter include = getMatchingConf().getInclude();
    if (include.getAttribute() instanceof LinkedHashMap<?, ?>) {
      LinkedHashMap<String, LinkedHashMap<String, String>> attribute = (LinkedHashMap<String, LinkedHashMap<String, String>>) (include.getAttribute());
      metricType = attribute.get(subAttributeName).get(METRIC_TYPE);
      if (metricType == null) {
        metricType = attribute.get(subAttributeName).get("type");
      }
    }

    if (metricType == null) {
      metricType = "gauge";
    }

    return metricType;
  }

  private Object getValue(String subAttribute) throws AttributeNotFoundException, InstanceNotFoundException,
          MBeanException, ReflectionException, IOException {

    Object value = this.getJmxValue();
    String attributeType = getAttribute().getType();

    if ("javax.management.openmbean.CompositeData".equals(attributeType)) {
      CompositeData data = (CompositeData) value;
      return data.get(subAttribute);
    } else if (("java.util.HashMap".equals(attributeType)) || ("java.util.Map".equals(attributeType))) {
      Map<String, Object> data = (Map<String, Object>) value;
      return data.get(subAttribute);
    }
    throw new NumberFormatException();
  }

  @Override
  public boolean match(JMXConfiguration configuration) {
    if (!matchDomain(configuration)
            || !matchBean(configuration)
            || excludeMatchDomain(configuration)
            || excludeMatchBean(configuration)) {
      return false;
    }

    try {
      populateSubAttributeList(getJmxValue());
    } catch (Exception e) {
      return false;
    }

    return matchAttribute(configuration) && !excludeMatchAttribute(configuration);
  }

  private void populateSubAttributeList(Object attributeValue) {
    String attributeType = getAttribute().getType();
    if ("javax.management.openmbean.CompositeData".equals(attributeType)) {
      CompositeData data = (CompositeData) attributeValue;
      for (String key : data.getCompositeType().keySet()) {
        this.subAttributeList.put(key, new HashMap<String, Object>());
      }
    } else if (("java.util.HashMap".equals(attributeType)) || ("java.util.Map".equals(attributeType))) {
      Map<String, Double> data = (Map<String, Double>) attributeValue;
      for (String key : data.keySet()) {
        this.subAttributeList.put(key, new HashMap<String, Object>());
      }
    }
  }

  private boolean excludeMatchAttribute(JMXConfiguration configuration) {
    JMXFilter exclude = configuration.getExclude();
    if (exclude == null) return false;
    if (exclude.getAttribute() != null && matchSubAttribute(exclude, getAttributeName(), false)) {
      return true;
    }

    Iterator<String> it = subAttributeList.keySet().iterator();
    while (it.hasNext()) {
      String subAttribute = it.next();
      if (matchSubAttribute(exclude, getAttributeName() + "." + subAttribute, false)) {
        it.remove();
      }
    }
    return subAttributeList.size() <= 0;
  }

  private boolean matchAttribute(JMXConfiguration configuration) {
    if (matchSubAttribute(configuration.getInclude(), getAttributeName(), true)) {
      return true;
    }

    Iterator<String> it = subAttributeList.keySet().iterator();

    while (it.hasNext()) {
      String subAttribute = it.next();
      if (!matchSubAttribute(configuration.getInclude(), getAttributeName() + "." + subAttribute, true)) {
        it.remove();
      }
    }

    return subAttributeList.size() > 0;
  }

  private boolean matchSubAttribute(JMXFilter params, String subAttributeName, boolean matchOnEmpty) {
    if ((params.getAttribute() instanceof LinkedHashMap<?, ?>)
            && ((LinkedHashMap<String, Object>) (params.getAttribute())).containsKey(subAttributeName)) {
      return true;
    } else if ((params.getAttribute() instanceof ArrayList<?>
            && ((ArrayList<String>) (params.getAttribute())).contains(subAttributeName))) {
      return true;
    } else if (params.getAttribute() == null) {
      return matchOnEmpty;
    }
    return false;

  }

}
