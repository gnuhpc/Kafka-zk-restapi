package org.gnuhpc.bigdata.model;

import java.util.List;
import lombok.Builder;
import org.gnuhpc.bigdata.constant.ZkServerMode;

@Builder
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
  private final String msg;
}
