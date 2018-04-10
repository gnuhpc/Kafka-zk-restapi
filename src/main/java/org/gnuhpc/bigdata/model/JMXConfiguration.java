package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.*;

@Getter
@Setter
public class JMXConfiguration {
  private JMXFilter include;
  private JMXFilter exclude;

  /**
   * Access JMXConfiguration elements more easily
   *
   * Also provides helper methods to extract common information among JMXFilters.
   */
  @JsonCreator
  public JMXConfiguration(@JsonProperty("include") JMXFilter include, @JsonProperty("exclude") JMXFilter exclude) {
    this.include = include;
    this.exclude = exclude;
  }

  private Boolean hasInclude(){
    return getInclude() != null;
  }

  /**
   * JMXFilter a configuration list to keep the ones with `include` JMXFilters.
   *
   * @param configurationList         the configuration list to JMXFilter
   *
   * @return                          a configuration list
   */
  private static LinkedList<JMXConfiguration> getIncludeConfigurationList(LinkedList<JMXConfiguration> configurationList){
    LinkedList<JMXConfiguration> includeConfigList = new LinkedList<JMXConfiguration>(configurationList);
    Iterator<JMXConfiguration> confItr = includeConfigList.iterator();

    while(confItr.hasNext()) {
      JMXConfiguration conf = confItr.next();
      if (!conf.hasInclude()) {
        confItr.remove();
      }
    }
    return includeConfigList;
  }

  /**
   * Extract `include` JMXFilters from the configuration list and index then by domain name.
   *
   * @param configurationList         the configuration list to process
   *
   * @return                          JMXFilters by domain name
   */
  private static HashMap<String, LinkedList<JMXFilter>> getIncludeJMXFiltersByDomain(LinkedList<JMXConfiguration> configurationList){
    HashMap<String, LinkedList<JMXFilter>> includeJMXFiltersByDomain = new HashMap<String, LinkedList<JMXFilter>>();

    for (JMXConfiguration conf : configurationList) {
      JMXFilter JMXFilter = conf.getInclude();
      LinkedList<JMXFilter> JMXFilters = new LinkedList<JMXFilter>();

      // Convert bean name, to a proper JMXFilter, i.e. a hash
      if (!JMXFilter.isEmptyBeanName()) {
        ArrayList<String> beanNames = JMXFilter.getBeanNames();

        for (String beanName : beanNames) {
          String[] splitBeanName = beanName.split(":");
          String domain = splitBeanName[0];
          String rawBeanParameters = splitBeanName[1];
          HashMap<String, String> beanParametersHash = JMXAttribute.getBeanParametersHash(rawBeanParameters);
          beanParametersHash.put("domain", domain);
          JMXFilters.add(new JMXFilter(beanParametersHash));
        }
      } else {
        JMXFilters.add(JMXFilter);
      }

      for (JMXFilter f: JMXFilters) {
        //  Retrieve the existing JMXFilters for the domain, add the new JMXFilters
        LinkedList<JMXFilter> domainJMXFilters;
        String domainName = f.getDomain();

        if (includeJMXFiltersByDomain.containsKey(domainName)) {
          domainJMXFilters = includeJMXFiltersByDomain.get(domainName);
        } else {
          domainJMXFilters = new LinkedList<JMXFilter>();
        }

        domainJMXFilters.add(f);
        includeJMXFiltersByDomain.put(domainName, domainJMXFilters);
      }
    }
    return includeJMXFiltersByDomain;
  }

  /**
   * Extract, among JMXFilters, bean key parameters in common.
   *
   * @param JMXFiltersByDomain       JMXFilters by domain name
   *
   * @return                      common bean key parameters by domain name
   */
  private static HashMap<String, Set<String>> getCommonBeanKeysByDomain(HashMap<String, LinkedList<JMXFilter>> JMXFiltersByDomain){
    HashMap<String, Set<String>> beanKeysIntersectionByDomain = new HashMap<String,Set<String>>();

    for (Map.Entry<String, LinkedList<JMXFilter>> JMXFiltersEntry : JMXFiltersByDomain.entrySet()) {
      String domainName = JMXFiltersEntry.getKey();
      LinkedList<JMXFilter> mJMXFilters= JMXFiltersEntry.getValue();

      // Compute keys intersection
      Set<String> keysIntersection = new HashSet<String>(mJMXFilters.getFirst().keySet());

      for (JMXFilter f: mJMXFilters) {
        keysIntersection.retainAll(f.keySet());
      }

      // Remove special parameters
      for(String param : JMXAttribute.getExcludedBeanParams()){
        keysIntersection.remove(param);
      }
      beanKeysIntersectionByDomain.put(domainName, keysIntersection);
    }

    return beanKeysIntersectionByDomain;
  }

  /**
   * Build a map of common bean keys->values, with the specified bean keys, among the given JMXFilters.
   *
   * @param beanKeysByDomain      bean keys by domain name
   * @param JMXFiltersByDomain       JMXFilters by domain name
   *
   * @return                      bean pattern (keys->values) by domain name
   */
  private static HashMap<String, LinkedHashMap<String, String>> getCommonScopeByDomain(HashMap<String, Set<String>> beanKeysByDomain, HashMap<String, LinkedList<JMXFilter>> JMXFiltersByDomain){
    // Compute a common scope a among JMXFilters by domain name
    HashMap<String, LinkedHashMap<String, String>> commonScopeByDomain = new HashMap<String, LinkedHashMap<String, String>>();

    for (Map.Entry<String, Set<String>> commonParametersByDomainEntry : beanKeysByDomain.entrySet()) {
      String domainName = commonParametersByDomainEntry.getKey();
      Set<String> commonParameters = commonParametersByDomainEntry.getValue();
      LinkedList<JMXFilter> JMXFilters = JMXFiltersByDomain.get(domainName);
      LinkedHashMap<String, String> commonScope = new LinkedHashMap<String, String>();

      for (String parameter : commonParameters) {
        // Check if all values associated with the parameters are the same
        String commonValue = null;
        Boolean hasCommonValue = true;

        for (JMXFilter f : JMXFilters) {
          ArrayList<String> parameterValues = f.getParameterValues(parameter);

          if (parameterValues.size() != 1 || (commonValue != null && !commonValue.equals(parameterValues.get(0)))) {
            hasCommonValue = false;
            break;
          }
          commonValue = parameterValues.get(0);

        }
        if (hasCommonValue) {
          commonScope.put(parameter, commonValue);
        }
      }
      commonScopeByDomain.put(domainName, commonScope);
    }

    return commonScopeByDomain;
  }

  /**
   * Stringify a bean pattern.
   *
   * @param domain                domain name
   * @param beanScope             map of bean keys-> values
   *
   * @return                      string pattern identifying the bean scope
   */
  private static String beanScopeToString(String domain, LinkedHashMap<String, String> beanScope){
    String result = "";

    // Domain
    domain = (domain != null) ? domain : "*";
    result += domain + ":";

    // Scope parameters
    for (Map.Entry<String, String> beanScopeEntry : beanScope.entrySet()) {
      String param = beanScopeEntry.getKey();
      String value = beanScopeEntry.getValue();

      result += param + "=" + value + ",";
    }
    result += "*";

    return result;
  }

  /**
   * Find, among the JMXConfiguration list, a potential common bean pattern by domain name.
   *
   * @param JMXConfigurationList         the JMXConfiguration list to process
   *
   * @return                          common bean pattern strings
   */
  public static LinkedList<String> getGreatestCommonScopes(LinkedList<JMXConfiguration> JMXConfigurationList){
    LinkedList<String> result = new LinkedList<String>();
    if (JMXConfigurationList == null || JMXConfigurationList.isEmpty()) {
      return result;
    }
    LinkedList<JMXConfiguration> includeConfigList = getIncludeConfigurationList(JMXConfigurationList);
    HashMap<String, LinkedList<JMXFilter>> includeJMXFiltersByDomain = getIncludeJMXFiltersByDomain(includeConfigList);
    HashMap<String, Set<String>> parametersIntersectionByDomain = getCommonBeanKeysByDomain(includeJMXFiltersByDomain);
    HashMap<String, LinkedHashMap<String, String>> commonBeanScopeByDomain = getCommonScopeByDomain(parametersIntersectionByDomain, includeJMXFiltersByDomain);

    for (Map.Entry<String, LinkedHashMap<String, String>> beanScopeEntry: commonBeanScopeByDomain.entrySet()) {
      String domain = beanScopeEntry.getKey();
      LinkedHashMap<String, String> beanScope = beanScopeEntry.getValue();

      result.add(beanScopeToString(domain, beanScope));
    }

    return result;
  }
}
