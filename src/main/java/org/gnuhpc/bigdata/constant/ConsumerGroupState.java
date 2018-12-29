package org.gnuhpc.bigdata.constant;

import java.util.HashMap;

public enum ConsumerGroupState {
  UNKNOWN("Unknown"),
  PREPARING_REBALANCE("PreparingRebalance"),
  COMPLETING_REBALANCE("CompletingRebalance"),
  STABLE("Stable"),
  DEAD("Dead"),
  EMPTY("Empty");

  private static final HashMap<String, ConsumerGroupState> NAME_TO_ENUM;

  static {
    NAME_TO_ENUM = new HashMap<>();
    for (ConsumerGroupState state : ConsumerGroupState.values()) {
      NAME_TO_ENUM.put(state.name, state);
    }
  }

  private final String name;

  ConsumerGroupState(String name) {
    this.name = name;
  }

  /** Parse a string into a consumer group state. */
  public static ConsumerGroupState parse(String name) {
    ConsumerGroupState state = NAME_TO_ENUM.get(name);
    return state == null ? UNKNOWN : state;
  }

  @Override
  public String toString() {
    return name;
  }
}
