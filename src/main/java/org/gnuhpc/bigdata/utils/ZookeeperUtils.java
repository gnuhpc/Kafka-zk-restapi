package org.gnuhpc.bigdata.utils;

import com.google.common.base.Charsets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.data.Stat;
import org.gnuhpc.bigdata.config.ZookeeperConfig;
import org.gnuhpc.bigdata.constant.ZkServerMode;
import org.gnuhpc.bigdata.exception.ServiceNotAvailableException;
import org.gnuhpc.bigdata.model.ZkServerClient;
import org.gnuhpc.bigdata.model.ZkServerEnvironment;
import org.gnuhpc.bigdata.model.ZkServerStat;
import org.gnuhpc.bigdata.validator.ZkNodePathExistConstraint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.validation.annotation.Validated;

/** Created by huangpengcheng on 2016/7/21 0021. */
@Log4j
@Setter
@Getter
@Validated
public class ZookeeperUtils {

  // For Stat Command parse
  private static final String ATTRIBUTE_DELIMITER = "=";
  private static final String PROP_DELIMITER = ":";
  private final Pattern versionLinePattern =
      Pattern.compile(".*: (\\d+\\.\\d+\\.\\d+.*),.* built on (.*)");
  private final Pattern ipv4ClientLinePattern =
      Pattern.compile("/(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)\\[(\\d+)\\]\\((.*)");
  private final Pattern latenciesPattern = Pattern.compile(".*: (-?\\d+)/(-?\\d+)/(-?\\d+)");

  @Lazy
  @Autowired private ZookeeperConfig zookeeperConfig;

  /** 初始sleep时间(毫秒). */
  private static final int BASE_SLEEP_TIME = 1000;
  /** 最大重试次数. */
  private static final int MAX_RETRIES_COUNT = 5;
  /** 最大sleep时间. */
  private static final int MAX_SLEEP_TIME = 60000;

  private static final int SESSION_TIMEOUT = 5000;
  private static final int CONNECTION_TIMEOUT = 5000;
  private ZkUtils zkUtils;
  private CuratorFramework curatorClient = null;
  private KafkaZkClient kafkaZkClient = null;
  private AdminZkClient adminZkClient = null;

  public void init() {
    //1.设置重试策略,重试时间计算策略sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME, MAX_RETRIES_COUNT, MAX_SLEEP_TIME);

    //2.初始化客户端
    curatorClient = CuratorFrameworkFactory.builder()
        .connectString(zookeeperConfig.getUris())
        .sessionTimeoutMs(SESSION_TIMEOUT)
        .connectionTimeoutMs(CONNECTION_TIMEOUT)
        .retryPolicy(retryPolicy)
//                .namespace("kafka-rest")        //命名空间隔离
        .build();
    curatorClient.start();
    try {
      curatorClient.blockUntilConnected();
      log.info("Zookeeper:" + zookeeperConfig.getUris() + " Connected.Continue...");
      kafkaZkClient =
          KafkaZkClient.apply(
              zookeeperConfig.getUris(),
              false,
              SESSION_TIMEOUT,
              CONNECTION_TIMEOUT,
              Integer.MAX_VALUE,
              Time.SYSTEM,
              "kafka.zk.rest",
              "rest");
      adminZkClient = new AdminZkClient(kafkaZkClient);
      ZkClient zkClient = new ZkClient(zookeeperConfig.getUris(), SESSION_TIMEOUT, CONNECTION_TIMEOUT, ZKStringSerializer$.MODULE$);
      zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConfig.getUris()), false);
      log.info("Zkutils" + zkUtils.toString());
    } catch (InterruptedException e) {
      e.printStackTrace();
      log.error("Exception:", e);
    }
  }

  public void destroy() {
    log.info("zookeeper closed.");
  }

  public List<String> executeCommand(String host, Integer port, String command)
      throws ServiceNotAvailableException {
    Socket socket;
    try {
      socket = new Socket();
      socket.connect(new InetSocketAddress(host, port), CONNECTION_TIMEOUT);
    } catch (IOException e) {
      throw new ServiceNotAvailableException(
          "zookeeper",
          ZkServerMode.Down,
          "could not connect to host: " + host + " and port: " + port);
    }

    try {
      IOUtils.write(command + "\n", socket.getOutputStream());
    } catch (IOException e) {
      throw new ServiceNotAvailableException(
          "zookeeper",
          ZkServerMode.Unknow,
          "could not write to host: " + host + " and port: " + port + ", command: " + command);
    }

    try {
      return IOUtils.readLines(socket.getInputStream());
    } catch (IOException e) {
      throw new ServiceNotAvailableException(
          "zookeeper",
          ZkServerMode.Unknow,
          "could not read from host: " + host + " and port: " + port + ", command: " + command);
    } finally {
      try {
        socket.close();
      } catch (IOException e) {
        throw new IllegalStateException("Error disconnecting from host", e);
      }
    }
  }

  public ZkServerStat parseStatResult(List<String> lines) {
    Iterator<String> iterator = lines.iterator();

    Matcher versionMatcher = versionLinePattern.matcher(iterator.next());
    versionMatcher.find();
    String version = versionMatcher.group(1);
    String buildDate = versionMatcher.group(2);
    iterator.next(); // Clients:
    List<ZkServerClient> clients = parseClientLines(iterator);

    Matcher latenciesMatcher = latenciesPattern.matcher(iterator.next());
    latenciesMatcher.find();
    Integer minLatency = Integer.parseInt(latenciesMatcher.group(1));
    Integer avgLatency = Integer.parseInt(latenciesMatcher.group(2));
    Integer maxLatency = Integer.parseInt(latenciesMatcher.group(3));

    Integer received = parseIntFromLine(iterator.next(), PROP_DELIMITER);
    Integer sent = parseIntFromLine(iterator.next(), PROP_DELIMITER);
    Integer connections = parseIntFromLine(iterator.next(), PROP_DELIMITER);
    Integer outstanding = parseIntFromLine(iterator.next(), PROP_DELIMITER);
    String zxid = parseStringFromLine(iterator.next(), PROP_DELIMITER);
    ZkServerMode mode =
        ZkServerMode.valueOf(
            StringUtils.capitalize(parseStringFromLine(iterator.next(), PROP_DELIMITER)));
    Integer nodeCount = parseIntFromLine(iterator.next(), PROP_DELIMITER);

    return ZkServerStat.builder()
        .version(version)
        .buildDate(buildDate)
        .clients(clients)
        .minLatency(minLatency)
        .avgLatency(avgLatency)
        .maxLatency(maxLatency)
        .received(received)
        .sent(sent)
        .connections(connections)
        .outstanding(outstanding)
        .zxId(zxid)
        .mode(mode)
        .nodes(nodeCount)
        .build();
  }

  private List<ZkServerClient> parseClientLines(Iterator<String> iterator) {
    return parseClients(iterator);
  }

  private List<ZkServerClient> parseClients(final Iterator<String> iterator) {
    final List<String> clientLines = new LinkedList<>();
    String clientLine = iterator.next();
    while (iterator.hasNext() && clientLine.trim().length() > 0 && clientLine.startsWith(" /")) {
      clientLines.add(clientLine);
      clientLine = iterator.next();
    }
    return createClients(clientLines);
  }

  private List<ZkServerClient> createClients(List<String> clientLines) {
    List<ZkServerClient> clients = new LinkedList<>();
    for (String clientLine : clientLines) {
      if (!StringUtils.isWhitespace(clientLine)) {
        Matcher matcher = ipv4ClientLinePattern.matcher(clientLine);
        // Skip the ipv6 address
        if (matcher.find()) {
          clients.add(parseClient(matcher));
        }
      }
    }
    return clients;
  }

  private String parseStringFromLine(String line, String delimiter) {
    return StringUtils.split(line, delimiter)[1].trim();
  }

  private Integer parseIntFromLine(String line, String delimiter) {
    final String value = StringUtils.split(line, delimiter)[1];
    return Integer.parseInt(value.trim());
  }

  private ZkServerClient parseClient(Matcher matcher) {
    String host = matcher.group(1);
    Integer port = Integer.parseInt(matcher.group(2));
    Integer ops = Integer.parseInt(matcher.group(3));
    String statSegment = matcher.group(4);
    if (statSegment.endsWith(")")) {
      statSegment = statSegment.substring(0, statSegment.length() - 1);
    }
    String[] stats = StringUtils.split(statSegment, ",");
    Integer queued = parseIntFromLine(stats[0], ATTRIBUTE_DELIMITER);
    Integer received = parseIntFromLine(stats[1], ATTRIBUTE_DELIMITER);
    Integer sent = parseIntFromLine(stats[2], ATTRIBUTE_DELIMITER);
    ZkServerClient client = new ZkServerClient(host, port, ops, queued, sent, received);
    return client;
  }

  public ZkServerEnvironment parseEnvResult(final List<String> result) {
    Iterator<String> iterator = result.iterator();
    iterator.next();
    ZkServerEnvironment environment = new ZkServerEnvironment();
    while (iterator.hasNext()) {
      String line = iterator.next();
      String[] parts = StringUtils.split(line, "=");
      environment.add(parts[0], parts[1]);
    }
    return environment;
  }

  public List<String> lsPath(@ZkNodePathExistConstraint String path) {
    List<String> stringList = null;

    try {
      stringList = curatorClient.getChildren().forPath(path);
    } catch (Exception e) {
      log.error("ls path fail! path: " + path + ", error: {}" + e);
    }

    return stringList;
  }

  public String getNodeData(@ZkNodePathExistConstraint String path) {
    String nodeData = null;

    try {
      GetDataBuilder dataBuilder = curatorClient.getData();
      byte[] bytes = dataBuilder.forPath(path);
      nodeData = (bytes != null) ? (new String(bytes, Charsets.UTF_8)) : (null);
    } catch (Exception e) {
      log.error("get node data fail! path: " + path + ", error: {}" + e);
    }

    return nodeData;
  }

  public Stat getNodePathStat(String path) {
    Stat stat = null;

    try {
      stat = curatorClient.checkExists().forPath(path);
    } catch (Exception e) {
      log.error("get node data fail! path: " + path + ", error: {}" + e);
    }

    return stat;
  }

  public String getState() {

    String stateStr = curatorClient.getState().toString();

    return stateStr;
  }

  public boolean isConnected(CuratorFramework curatorClient) {
    return curatorClient.getZookeeperClient().isConnected();
  }
}
