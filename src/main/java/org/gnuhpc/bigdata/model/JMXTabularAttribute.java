package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.InvalidKeyException;
import javax.management.openmbean.TabularData;
import java.io.IOException;
import java.util.*;

@Getter
@Setter
@Log4j
public class JMXTabularAttribute extends JMXAttribute {

  private HashMap<String, HashMap<String, HashMap<String, Object>>> subAttributeList;

  public JMXTabularAttribute(MBeanAttributeInfo attribute, ObjectName beanName, MBeanServerConnection connection) {
    super(attribute, beanName, connection);
    subAttributeList = new HashMap<String, HashMap<String, HashMap<String, Object>>>();
  }

  @Override
  public LinkedList<HashMap<String, Object>> getMetrics() throws AttributeNotFoundException,
          InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    LinkedList<HashMap<String, Object>> metrics = new LinkedList<HashMap<String, Object>>();
    HashMap<String, LinkedList<HashMap<String, Object>>> subMetrics = new HashMap<String,
            LinkedList<HashMap<String, Object>>>();

    for (String dataKey : subAttributeList.keySet()) {
      HashMap<String, HashMap<String, Object>> subSub = subAttributeList.get(dataKey);
      for (String metricKey : subSub.keySet()) {
        String fullMetricKey = getAttributeName() + "." + metricKey;

        HashMap<String, Object> metric = subSub.get(metricKey);
        metric.put("domain", getBeanName().getDomain());
        metric.put("beanName", getBeanName().getCanonicalName());
        metric.put("attributeName", fullMetricKey);
        if (metric.get(ALIAS) == null) {
          metric.put(ALIAS, convertMetricName(getAlias(metricKey)));
        }

        if (metric.get(METRIC_TYPE) == null) {
          metric.put(METRIC_TYPE, getMetricType(metricKey));
        }

        /*
        if (metric.get("tags") == null) {
          metric.put("tags", getTags(dataKey, metricKey));
        }*/

        metric.put("value", castToDouble(getValue(dataKey, metricKey), null));

        if(!subMetrics.containsKey(fullMetricKey)) {
          subMetrics.put(fullMetricKey, new LinkedList<HashMap<String, Object>>());
        }
        subMetrics.get(fullMetricKey).add(metric);
      }
    }

    for (String key : subMetrics.keySet()) {
      // only add explicitly included metrics
      if (getAttributesFor(key) != null) {
        metrics.addAll(sortAndFilter(key, subMetrics.get(key)));
      }
    }

    return metrics;
  }

  private Object getMetricType(String subAttribute) {
    String subAttributeName = getAttribute().getName() + "." + subAttribute;
    String metricType = null;

    JMXFilter include = getMatchingConf().getInclude();
    if (include.getAttribute() instanceof LinkedHashMap<?, ?>) {
      LinkedHashMap<String, LinkedHashMap<String, String>> attribute = (LinkedHashMap<String,
              LinkedHashMap<String, String>>) (include.getAttribute());
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

  private Object getValue(String key, String subAttribute) throws AttributeNotFoundException,
          InstanceNotFoundException,
          MBeanException, ReflectionException, IOException {

    try{
      Object value = this.getJmxValue();
      String attributeType = getAttribute().getType();

      TabularData data = (TabularData) value;
      for (Object rowKey : data.keySet()) {
        Collection keys = (Collection) rowKey;
        String pathKey = getMultiKey(keys);
        if (key.equals(pathKey)) {
          CompositeData compositeData = data.get(keys.toArray());
          if (subAttribute.contains(".")) {
            // walk down the path
            Object o;
            for (String subPathKey : subAttribute.split("\\.")) {
              o = compositeData.get(subPathKey);
              if (o instanceof CompositeData) {
                compositeData = (CompositeData) o;
              } else {
                return compositeData.get(subPathKey);
              }
            }
          } else {
            return compositeData.get(subAttribute);
          }
        }
      }
    }
    catch (InvalidKeyException e) {
      log.warn("`"+getAttribute().getName()+"` attribute does not have a `"+subAttribute+"` key.");
      return null;
    }

    throw new NumberFormatException();
  }

  private Map<String, ?> getAttributesFor(String key) {
    JMXFilter include = getMatchingConf().getInclude();
    if (include != null) {
      Object includeAttribute = include.getAttribute();
      if (includeAttribute instanceof LinkedHashMap<?, ?>) {
        return (Map<String, ?>) ((Map)includeAttribute).get(key);
      }
    }
    return null;
  }

  private List<HashMap<String, Object>> sortAndFilter(String metricKey, LinkedList<HashMap<String, Object>>
          metrics) {
    Map<String, ?> attributes = getAttributesFor(metricKey);
    if (!attributes.containsKey("limit")) {
      return metrics;
    }
    Integer limit = (Integer) attributes.get("limit");
    if (metrics.size() <= limit) {
      return metrics;
    }
    MetricComparator comp = new MetricComparator();
    Collections.sort(metrics, comp);
    String sort = (String) attributes.get("sort");
    if (sort == null || sort.equals("desc")) {
      metrics.subList(0, limit).clear();
    } else {
      metrics.subList(metrics.size() - limit, metrics.size()).clear();
    }
    return metrics;
  }

  private class MetricComparator implements Comparator<HashMap<String, Object>> {
    public int compare(HashMap<String, Object> o1, HashMap<String, Object> o2) {
      Double v1 = (Double) o1.get("value");
      Double v2 = (Double) o2.get("value");
      return v1.compareTo(v2);
    }
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

    return matchAttribute(configuration);//TODO && !excludeMatchAttribute(configuration);
  }

  private void populateSubAttributeList(Object value) {
    TabularData data = (TabularData) value;
    for (Object rowKey : data.keySet()) {
      Collection keys = (Collection) rowKey;
      CompositeData compositeData = data.get(keys.toArray());
      String pathKey = getMultiKey(keys);
      HashMap<String, HashMap<String, Object>> subAttributes = new HashMap<String, HashMap<String, Object>>();
      for (String key : compositeData.getCompositeType().keySet()) {
        if (compositeData.get(key) instanceof CompositeData) {
          for (String subKey : ((CompositeData) compositeData.get(key)).getCompositeType().keySet()) {
            subAttributes.put(key + "." + subKey, new HashMap<String, Object>());
          }
        } else {
          subAttributes.put(key, new HashMap<String, Object>());
        }
      }
      subAttributeList.put(pathKey, subAttributes);
    }
  }

  private boolean matchAttribute(JMXConfiguration configuration) {
    if (matchSubAttribute(configuration.getInclude(), getAttributeName(), true)) {
      return true;
    }

    Iterator<String> it1 = subAttributeList.keySet().iterator();
    while (it1.hasNext()) {
      String key = it1.next();
      HashMap<String, HashMap<String, Object>> subSub = subAttributeList.get(key);
      Iterator<String> it2 = subSub.keySet().iterator();
      while (it2.hasNext()) {
        String subKey = it2.next();
        if (!matchSubAttribute(configuration.getInclude(), getAttributeName() + "." + subKey, true)) {
          it2.remove();
        }
      }
      if (subSub.size() <= 0) {
        it1.remove();
      }
    }

    return subAttributeList.size() > 0;
  }

  private String getMultiKey(Collection keys) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Object key : keys) {
      if (!first) { sb.append(","); }
      // I hope these have sane toString() methods
      sb.append(key.toString());
      first = false;
    }
    return sb.toString();
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
