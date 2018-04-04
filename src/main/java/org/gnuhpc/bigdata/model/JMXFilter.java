package org.gnuhpc.bigdata.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

@Getter
@Setter
public class JMXFilter {
  HashMap<String, Object> filter;
  Pattern domainRegex;
  ArrayList<Pattern> beanRegexes = null;
  ArrayList<String> excludeTags = null;
  HashMap<String, String> additionalTags = null;

  /**
   * A simple class to manipulate include/exclude filter elements more easily
   * A filter may contain:
   * - A domain (key: 'domain') or a domain regex (key: 'domain_regex')
   * - Bean names (key: 'bean' or 'bean_name') or bean regexes (key: 'bean_regex')
   * - Attributes (key: 'attribute')
   * - Additional bean parameters (other keys)
   */
  @JsonCreator
  @SuppressWarnings("unchecked")
  public JMXFilter(Object filter) {
    HashMap<String, Object> castFilter;
    if (filter != null) {
      castFilter = (HashMap<String, Object>) filter;
    } else {
      castFilter = new HashMap<String, Object>();
    }
    this.filter = castFilter;
  }

  public String toString() {
    return this.filter.toString();
  }

  public Set<String> keySet() {
    return filter.keySet();
  }

  @SuppressWarnings({ "unchecked", "serial" })
  private static ArrayList<String> toStringArrayList(final Object toCast) {
    // Return object as an ArrayList wherever it's defined as
    // list or not
    //
    // ### Example
    // object:
    //                  - firstValue
    //                  - secondValue
    // ### OR
    // object: singleValue
    // ###
    try{
      return (ArrayList<String>) toCast;
    } catch (ClassCastException e){
      return new ArrayList<String>() {{
        add(((String) toCast));
      }};
    }
  }


  public ArrayList<String> getBeanNames() {
    if (isEmptyBeanName()){
      return new ArrayList<String>();
    }
    final Object beanNames = (filter.get("bean") != null) ? filter.get("bean") : filter.get("bean_name");
    // Return bean names as an ArrayList wherever it's defined as
    // list or not
    //
    // ### Example
    // bean:
    //                  - org.apache.cassandra.db:type=Caches,keyspace=system,cache=HintsColumnFamilyKeyCache
    //                  - org.datadog.jmxfetch.test:type=type=SimpleTestJavaApp
    // ### OR
    // bean: org.datadog.jmxfetch.test:type=type=SimpleTestJavaApp
    // ###
    return toStringArrayList(beanNames);
  }

  private static ArrayList<Pattern> toPatternArrayList(final Object toCast) {
    ArrayList<Pattern> patternArrayList = new ArrayList<Pattern>();
    ArrayList<String> stringArrayList = toStringArrayList(toCast);
    for (String string : stringArrayList) {
      patternArrayList.add(Pattern.compile(string));
    }

    return patternArrayList;
  }

  public ArrayList<Pattern> getBeanRegexes() {
    // Return bean regexes as an ArrayList of Pattern whether it's defined as
    // a list or not

    if (this.beanRegexes == null) {
      if (filter.get("bean_regex") == null){
        this.beanRegexes = new ArrayList<Pattern>();
      } else {
        final Object beanRegexNames = filter.get("bean_regex");
        this.beanRegexes = toPatternArrayList(beanRegexNames);
      }
    }

    return this.beanRegexes;
  }

  public ArrayList<String> getExcludeTags() {
    // Return excluded tags  as an ArrayList whether it's defined as a list or not

    if (this.excludeTags == null) {
      if (filter.get("exclude_tags") == null){
        this.excludeTags = new ArrayList<String>();
      } else {
        final Object exclude_tags = filter.get("exclude_tags");
        this.excludeTags = toStringArrayList(exclude_tags);
      }
    }

    return this.excludeTags;
  }

  public HashMap<String, String> getAdditionalTags() {
    // Return additional tags
    if (this.additionalTags == null) {
      if (filter.get("tags") == null){
        this.additionalTags = new HashMap<String, String>();
      } else {
        this.additionalTags = (HashMap<String, String>)filter.get("tags");
      }
    }

    return this.additionalTags;
  }

  public String getDomain() {
    return (String) filter.get("domain");
  }

  public Pattern getDomainRegex() {
    if (this.filter.get("domain_regex") == null) {
      return null;
    }

    if (this.domainRegex == null) {
      this.domainRegex = Pattern.compile((String) this.filter.get("domain_regex"));
    }

    return this.domainRegex;
  }

  public Object getAttribute() {
    return filter.get("attribute");
  }

  public ArrayList<String> getParameterValues(String parameterName) {
    // Return bean attributes values as an ArrayList wherever it's defined as
    // list or not
    //
    // ### Example
    // bean_parameter:
    //                  - exampleType1
    //                  - exampleType2
    // ### OR
    // bean_parameter: onlyOneType
    // ###
    final Object beanValues = filter.get(parameterName);
    return toStringArrayList(beanValues);
  }

  public boolean isEmptyBeanName() {
    return (filter.get("bean") == null && filter.get("bean_name") == null);
  }
}
