package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ZkServerClient {

  private final String host;
  private final Integer port;
  private final Integer ops;
  private final Integer queued;
  private final Integer received;
  private final Integer sent;
}
