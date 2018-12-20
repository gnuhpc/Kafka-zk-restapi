package org.gnuhpc.bigdata.model;

import lombok.Data;

@Data
public class Record {
  long offset;
  String key;
  String value;
  long timestamp;
}
