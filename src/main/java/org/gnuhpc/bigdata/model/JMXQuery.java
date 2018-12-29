package org.gnuhpc.bigdata.model;

import java.util.LinkedList;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class JMXQuery {

  private LinkedList<JMXConfiguration> filters;
}
