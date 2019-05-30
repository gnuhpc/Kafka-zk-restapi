package org.gnuhpc.bigdata.model;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
@Builder
public class SchemaRegistryMetadata {
  private String subject;
  private int id;
  private int version;
  private String schema;
}
