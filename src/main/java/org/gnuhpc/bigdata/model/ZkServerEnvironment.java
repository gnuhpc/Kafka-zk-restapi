package org.gnuhpc.bigdata.model;

import java.util.HashMap;
import java.util.Map;

public class ZkServerEnvironment {

  private final Map<String, String> attributes = new HashMap<String, String>();

  public void add(final String attribute, final String value) {
    attributes.put(attribute, value);
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }
}
