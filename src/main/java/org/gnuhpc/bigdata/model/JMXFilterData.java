package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;

@Getter
@Setter
public class JMXFilterData {
  HashMap<String, Object> filter;

  public JMXFilterData() {

  }

  public JMXFilterData(Object filter) {
    HashMap<String, Object> castFilter;
    if (filter != null) {
      castFilter = (HashMap<String, Object>) filter;
    } else {
      castFilter = new HashMap<String, Object>();
    }
    this.filter = castFilter;
  }

  public String getDomain() {
    return (String) filter.get("domain");
  }
}
