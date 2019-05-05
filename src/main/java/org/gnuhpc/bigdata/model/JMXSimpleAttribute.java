package org.gnuhpc.bigdata.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JMXSimpleAttribute extends JMXAttribute {

  private String metricType;

  public JMXSimpleAttribute(
      MBeanAttributeInfo attribute, ObjectName beanName, MBeanServerConnection connection) {
    super(attribute, beanName, connection);
  }

  @Override
  public LinkedList<HashMap<String, Object>> getMetrics()
      throws AttributeNotFoundException, InstanceNotFoundException, MBeanException,
          ReflectionException, IOException {
    HashMap<String, Object> metric = new HashMap<String, Object>();

    metric.put("domain", getBeanName().getDomain());
    metric.put("beanName", getBeanName().toString());
    metric.put("attributeName", getAttributeName());
    metric.put("alias", getAlias());
    metric.put("value", castToDouble(getValue(), null));
    // metric.put("tags", getTags());
    metric.put("metric_type", getMetricType());
    LinkedList<HashMap<String, Object>> metrics = new LinkedList<HashMap<String, Object>>();
    metrics.add(metric);
    return metrics;
  }

  public boolean match(JMXConfiguration configuration) {
    return matchDomain(configuration)
        && matchBean(configuration)
        && matchAttribute(configuration)
        && !(excludeMatchDomain(configuration)
            || excludeMatchBean(configuration)
            || excludeMatchAttribute(configuration));
  }

  private boolean matchAttribute(JMXConfiguration configuration) {
    JMXFilter include = configuration.getInclude();
    if (include.getAttribute() == null) {
      return true;
    } else if ((include.getAttribute() instanceof LinkedHashMap<?, ?>)
        && ((LinkedHashMap<String, Object>) (include.getAttribute()))
            .containsKey(getAttributeName())) {
      return true;

    } else if ((include.getAttribute() instanceof ArrayList<?>
        && ((ArrayList<String>) (include.getAttribute())).contains(getAttributeName()))) {
      return true;
    }

    return false;
  }

  private boolean excludeMatchAttribute(JMXConfiguration configuration) {
    JMXFilter exclude = configuration.getExclude();
    if (exclude == null) {
      return false;
    }
    if (exclude.getAttribute() == null) {
      return false;
    } else if ((exclude.getAttribute() instanceof LinkedHashMap<?, ?>)
        && ((LinkedHashMap<String, Object>) (exclude.getAttribute()))
            .containsKey(getAttributeName())) {
      return true;

    } else if ((exclude.getAttribute() instanceof ArrayList<?>
        && ((ArrayList<String>) (exclude.getAttribute())).contains(getAttributeName()))) {
      return true;
    }
    return false;
  }

  private Object getValue()
      throws AttributeNotFoundException, InstanceNotFoundException, MBeanException,
          ReflectionException, IOException, NumberFormatException {
    return this.getJmxValue();
  }

  private String getMetricType() {
    JMXFilter include = getMatchingConf().getInclude();
    if (metricType != null) {
      return metricType;
    } else if (include.getAttribute() instanceof LinkedHashMap<?, ?>) {
      LinkedHashMap<String, LinkedHashMap<String, String>> attribute =
          (LinkedHashMap<String, LinkedHashMap<String, String>>) (include.getAttribute());
      metricType = attribute.get(getAttributeName()).get(METRIC_TYPE);
      if (metricType == null) {
        metricType = attribute.get(getAttributeName()).get("type");
      }
    }

    if (metricType == null) { // Default to gauge
      metricType = "gauge";
    }

    return metricType;
  }
}
