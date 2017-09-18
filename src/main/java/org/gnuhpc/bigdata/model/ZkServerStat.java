package org.gnuhpc.bigdata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.gnuhpc.bigdata.constant.ZkServerMode;

import java.util.List;


@Data
@AllArgsConstructor
public class ZkServerStat {
  private final String version;
  private final String buildDate;
  private final List<ZkServerClient> clients;
  private final Integer minLatency;
  private final Integer avgLatency;
  private final Integer maxLatency;
  private final Integer received;
  private final Integer sent;
  private final Integer connections;
  private final Integer outstanding;
  private final String zxId;
  private final ZkServerMode mode;
  private final Integer nodes;
}
