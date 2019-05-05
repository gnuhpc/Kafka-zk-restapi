package org.gnuhpc.bigdata.utils;

import java.nio.ByteBuffer;

public class ByteUtils {

  private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

  public static byte[] longToBytes(long x) {
    buffer.putLong(0, x);
    return buffer.array();
  }

  public static long bytesToLong(byte[] bytes) {
    buffer.put(bytes, 0, bytes.length);
    buffer.flip(); // need flip
    return buffer.getLong();
  }
}
