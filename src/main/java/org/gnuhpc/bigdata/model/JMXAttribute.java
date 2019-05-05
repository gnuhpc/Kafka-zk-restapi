package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import javax.management.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public abstract class JMXAttribute {
  private MBeanAttributeInfo attribute;
  private ObjectName beanName;
  private MBeanServerConnection connection;
  private String attributeName;
  private String beanStringName;
  private String domain;
  private HashMap<String, String> beanParameters;
  private JMXConfiguration matchingConf;
  private LinkedHashMap<String, LinkedHashMap<Object, Object>> valueConversions = new LinkedHashMap<String, LinkedHashMap<Object, Object>>();
  private static final List<String> EXCLUDED_BEAN_PARAMS = Arrays.asList("domain", "domain_regex", "bean_name", "bean",
          "bean_regex", "attribute", "exclude_tags", "tags");
  protected static final String METRIC_TYPE = "metric_type";
  protected static final String ALIAS = "alias";
  private static final String FIRST_CAP_PATTERN = "(.)([A-Z][a-z]+)";
  private static final String ALL_CAP_PATTERN = "([a-z0-9])([A-Z])";
  private static final String METRIC_REPLACEMENT = "([^a-zA-Z0-9_.]+)|(^[^a-zA-Z]+)";
  private static final String DOT_UNDERSCORE = "_*\\._*";

  public JMXAttribute(MBeanAttributeInfo attribute, ObjectName beanName, MBeanServerConnection connection) {
    this.attribute = attribute;
    this.attributeName = attribute.getName();
    this.beanName = beanName;
    this.beanStringName = beanName.toString();
    this.connection = connection;
    // A bean name is formatted like that: org.apache.cassandra.db:type=Caches,keyspace=system,cache=HintsColumnFamilyKeyCache
    // i.e. : domain:bean_parameter1,bean_parameter2
    //Note: some beans have a ':' in the name. Example:  some.domain:name="some.bean.0.0.0.0:80.some-metric"
    int splitPosition = beanStringName.indexOf(':');
    String domain = beanStringName.substring(0, splitPosition);
    String beanParametersString = beanStringName.substring(splitPosition+1);
    this.domain = domain;
    this.matchingConf = null;

    HashMap<String, String> beanParametersHash = getBeanParametersHash(beanParametersString);
    //LinkedList<String> beanParametersList = getBeanParametersList(instanceName, beanParametersHash, instanceTags);
    this.beanParameters = beanParametersHash;
  }


  public abstract LinkedList<HashMap<String, Object>> getMetrics() throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException;

  /**
   * An abstract function implemented in the inherited classes JMXSimpleAttribute and JMXComplexAttribute
   *
   * @param conf Configuration a Configuration object that will be used to check if the JMX Attribute match this configuration
   * @return a boolean that tells if the attribute matches the configuration or not
   */
  public abstract boolean match(JMXConfiguration conf);

  public static HashMap<String, String> getBeanParametersHash(String beanParametersString) {
    String[] beanParameters = beanParametersString.split(",");
    HashMap<String, String> beanParamsMap = new HashMap<String, String>(beanParameters.length);
    for (String param : beanParameters) {
      String[] paramSplit = param.split("=");
      if (paramSplit.length > 1) {
        beanParamsMap.put(new String(paramSplit[0]), new String(paramSplit[1]));
      } else {
        beanParamsMap.put(new String(paramSplit[0]), "");
      }
    }

    return beanParamsMap;
  }

  boolean matchDomain(JMXConfiguration conf) {
    String includeDomain = conf.getInclude().getDomain();
    Pattern includeDomainRegex = conf.getInclude().getDomainRegex();

    return (includeDomain == null || includeDomain.equals(this.getDomain()))
            && (includeDomainRegex == null || includeDomainRegex.matcher(this.getDomain()).matches());
  }

  boolean matchBean(JMXConfiguration configuration) {
    return matchBeanName(configuration) && matchBeanRegex(configuration.getInclude(), true);
  }

  private boolean matchBeanName(JMXConfiguration configuration) {
    JMXFilter include = configuration.getInclude();

    if (!include.isEmptyBeanName() && !include.getBeanNames().contains(this.getBeanStringName())) {
      return false;
    }

    for (String bean_attr : include.keySet()) {
      if (EXCLUDED_BEAN_PARAMS.contains(bean_attr)) {
        continue;
      }

      ArrayList<String> beanValues = include.getParameterValues(bean_attr);

      if (beanParameters.get(bean_attr) == null || !(beanValues.contains(beanParameters.get(bean_attr)))){
        return false;
      }
    }
    return true;
  }

  private boolean matchBeanRegex(JMXFilter filter, boolean matchIfNoRegex) {
    if (filter == null) return matchIfNoRegex;
    ArrayList<Pattern> beanRegexes = filter.getBeanRegexes();
    if (beanRegexes.isEmpty()) {
      return matchIfNoRegex;
    }

    for (Pattern beanRegex : beanRegexes) {
      Matcher m = beanRegex.matcher(beanStringName);

      if(m.matches()) {
        for (int i = 0; i<= m.groupCount(); i++) {
          this.beanParameters.put(Integer.toString(i), m.group(i));
        }
        return true;
      }
    }
    return false;
  }

  boolean excludeMatchDomain(JMXConfiguration conf) {
    if (conf.getExclude() == null) return false;
    String excludeDomain = conf.getExclude().getDomain();
    Pattern excludeDomainRegex = conf.getExclude().getDomainRegex();

    return excludeDomain != null  && excludeDomain.equals(domain)
            || excludeDomainRegex != null && excludeDomainRegex.matcher(domain).matches();
  }

  boolean excludeMatchBean(JMXConfiguration configuration) {
    return excludeMatchBeanName(configuration) || matchBeanRegex(configuration.getExclude(), false);
  }

  private boolean excludeMatchBeanName(JMXConfiguration conf) {
    JMXFilter exclude = conf.getExclude();
    if (exclude == null) return false;
    ArrayList<String> beanNames = exclude.getBeanNames();

    if(beanNames.contains(beanStringName)){
      return true;
    }

    for (String bean_attr : exclude.keySet()) {
      if (EXCLUDED_BEAN_PARAMS.contains(bean_attr)) {
        continue;
      }

      if (beanParameters.get(bean_attr) == null) {
        continue;
      }

      ArrayList<String> beanValues = exclude.getParameterValues(bean_attr);
      for (String beanVal : beanValues) {
        if (beanParameters.get(bean_attr).equals(beanVal)) {
          return true;
        }
      }
    }
    return false;
  }

  Object getJmxValue() throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    return this.getConnection().getAttribute(this.getBeanName(), this.getAttribute().getName());
  }

  public static List<String> getExcludedBeanParams(){
    return EXCLUDED_BEAN_PARAMS;
  }

  double castToDouble(Object metricValue, String field) {
    Object value = convertMetricValue(metricValue, field);

    if (value instanceof String) {
      return Double.parseDouble((String) value);
    } else if (value instanceof Integer) {
      return new Double((Integer) (value));
    } else if (value instanceof AtomicInteger) {
      return new Double(((AtomicInteger) (value)).get());
    } else if (value instanceof AtomicLong) {
      Long l = ((AtomicLong) (value)).get();
      return l.doubleValue();
    } else if (value instanceof Double) {
      return (Double) value;
    } else if (value instanceof Boolean) {
      return ((Boolean) value ? 1.0 : 0.0);
    } else if (value instanceof Long) {
      Long l = new Long((Long) value);
      return l.doubleValue();
    } else if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else {
      try {
        return new Double((Double) value);
      } catch (Exception e) {
        throw new NumberFormatException();
      }
    }
  }

  Object convertMetricValue(Object metricValue, String field) {
    Object converted = metricValue;

    if (!getValueConversions(field).isEmpty()) {
      converted = getValueConversions(field).get(metricValue);
      if (converted == null && getValueConversions(field).get("default") != null) {
        converted = getValueConversions(field).get("default");
      }
    }

    return converted;
  }

  @SuppressWarnings("unchecked")
  HashMap<Object, Object> getValueConversions(String field) {
    String fullAttributeName =(field!=null)?(getAttribute().getName() + "." + field):(getAttribute().getName());
    if (valueConversions.get(fullAttributeName) == null) {
      Object includedAttribute = matchingConf.getInclude().getAttribute();
      if (includedAttribute instanceof LinkedHashMap<?, ?>) {
        LinkedHashMap<String, LinkedHashMap<Object, Object>> attribute =
                ((LinkedHashMap<String, LinkedHashMap<String, LinkedHashMap<Object, Object>>>) includedAttribute).get(fullAttributeName);

        if (attribute != null) {
          valueConversions.put(fullAttributeName, attribute.get("values"));
        }
      }
      if (valueConversions.get(fullAttributeName) == null) {
        valueConversions.put(fullAttributeName, new LinkedHashMap<Object, Object>());
      }
    }

    return valueConversions.get(fullAttributeName);
  }

  /**
   * Overload `getAlias` method.
   *
   * Note: used for `JMXSimpleAttribute` only, as `field` is null.
   */
  protected String getAlias(){
    return getAlias(null);
  }

  /**
   * Get attribute alias.
   *
   * In order, tries to:
   * * Use `alias_match` to generate an alias with a regular expression
   * * Use `alias` directly
   * * Create an generic alias prefixed with user's `metric_prefix` preference or default to `jmx`
   *
   * Argument(s):
   * * (Optional) `field`
   *   `Null` for `JMXSimpleAttribute`.
   */
  protected String getAlias(String field) {
    String alias = null;

    JMXFilter include = getMatchingConf().getInclude();

    String fullAttributeName =(field!=null)?(getAttribute().getName() + "." + field):(getAttribute().getName());

    if (include.getAttribute() instanceof LinkedHashMap<?, ?>) {
      LinkedHashMap<String, LinkedHashMap<String, String>> attribute = (LinkedHashMap<String, LinkedHashMap<String, String>>) (include.getAttribute());
      alias = getUserAlias(attribute, fullAttributeName);
    }

    //If still null - generate generic alias
    if (alias == null) {
      alias = "jmx." + getDomain() + "." + fullAttributeName;
    }

    return alias;
  }

  /**
   * Retrieve user defined alias. Substitute regular expression named groups.
   *
   * Example:
   *   ```
   *   bean: org.datadog.jmxfetch.test:foo=Bar,qux=Baz
   *   attribute:
   *     toto:
   *       alias: my.metric.$foo.$attribute
   *   ```
   *   returns a metric name `my.metric.bar.toto`
   */
  private String getUserAlias(LinkedHashMap<String, LinkedHashMap<String, String>> attribute, String fullAttributeName){
    String alias = attribute.get(fullAttributeName).get(ALIAS);
    if (alias == null) {
      return null;
    }

    alias = this.replaceByAlias(alias);

    // Attribute & domain
    alias = alias.replace("$attribute", fullAttributeName);
    alias = alias.replace("$domain", domain);

    return alias;
  }

  private String replaceByAlias(String alias){
    // Bean parameters
    for (Map.Entry<String, String> param : beanParameters.entrySet()) {
      alias = alias.replace("$" + param.getKey(), param.getValue());
    }
    return alias;
  }

  static String convertMetricName(String metricName) {
    metricName = metricName.replaceAll(FIRST_CAP_PATTERN, "$1_$2");
    metricName = metricName.replaceAll(ALL_CAP_PATTERN, "$1_$2").toLowerCase();
    metricName = metricName.replaceAll(METRIC_REPLACEMENT, "_");
    metricName = metricName.replaceAll(DOT_UNDERSCORE, ".").trim();
    return metricName;
  }
}