package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.admin.ConfigEntry.ConfigSource;

@Data
@AllArgsConstructor
public class CustomConfigEntry {

  private String name;
  private String value;
  private boolean isSensitive;
  private boolean isReadOnly;
  private ConfigSource source;
}
