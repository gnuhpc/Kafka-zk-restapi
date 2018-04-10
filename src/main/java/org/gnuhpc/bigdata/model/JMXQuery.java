package org.gnuhpc.bigdata.model;

import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;


@Getter
@Setter
public class JMXQuery {
  private LinkedList<JMXConfiguration> filters;
}
